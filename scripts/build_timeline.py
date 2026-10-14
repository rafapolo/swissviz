#!/usr/bin/env python3
"""Turn the SHAB events, the register entry dates and today's companies into the map's timeline file.

Each company becomes one lamp with the day it opened and the day it closed
(days since 1883-01-01; 65535 = unknown), placed on its building by the
swisstopo address register. Three sources, one lamp per company:
  1. SHAB events 2018-2026 (build/shab_events.jsonl): opening and closing, each at its address;
  2. a closing with no opening in the SHAB gets its opening from the cantonal register
     (data/registered.json.gz, see tasks/cantonal-register.md);
  3. an active company with no SHAB event (registered before mid-2018) gets a lamp at its
     current address (build/lindas_active.parquet, scripts/active_firms.py), lit from its
     register entry date.
Without an exact date, the estimate of data/opening_dates.json.gz (night-light branch) is used:
precision 0-1 the day, 2 the 1st of July of the year, 3 none. See tasks/linha-do-tempo-longa.md.

Output: data/timeline/events.bin.gz, little-endian, all columns n long:
    uint32 n
    int32  lat * 1e5      int32 lng * 1e5
    uint16 open day       uint16 close day
    uint8  legal form (ASCII letter, see T_DECODE in index.html)
plus data/timeline/names.json.gz (names in the same order, loaded on click)
and build/timeline_geo.parquet (every SHAB event with its precision, for checks).

Usage:
    python3 scripts/build_timeline.py
"""
import datetime as dt, gzip, json, os, struct, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))

EVENTS = os.environ.get("SHAB_EVENTS", "build/shab_events.jsonl")
REGISTERED = "data/registered.json.gz"
ACTIVE = "build/lindas_active.parquet"
ESTIMATES = os.path.expanduser(os.environ.get("OPENING_ESTIMATES", "~/Projetos/swissviz/data/opening_dates.json.gz"))
OUT = "data/timeline"
EPOCH = dt.date(1883, 1, 1)
NONE = 65535

# eCH-0097 legal form → one-letter code used by the site (scripts/repack.py)
LEGAL = {
    "0101": "E", "0103": "K", "0104": "O", "0105": "B", "0106": "A", "0107": "G",
    "0108": "N", "0109": "V", "0110": "S", "0111": "L", "0113": "B", "0114": "B",
    "0115": "B", "0116": "B", "0117": "I", "0118": "B", "0119": "H", "0151": "Z",
}
LAMP_COLS = ["name", "t", "lat", "lng", "o", "c"]


def day_of(yyyymmdd: int) -> int:
    """Days since EPOCH; dates before it count as day 0."""
    d = dt.date(yyyymmdd // 10000, yyyymmdd // 100 % 100, yyyymmdd % 100)
    return max(0, (d - EPOCH).days)


def entry_days(registered: dict, estimates: dict | None) -> dict:
    """uid (int) -> opening day: the register date, else an estimate (date, precision)
    of precision 0-1 (the day) or 2 (the year, placed on 1 July); precision 3 or none: absent."""
    out = {}
    for uid, (date, prec) in (estimates or {}).items():
        if prec in (0, 1) and date > 10_000_000:
            out[uid] = day_of(date)
        elif prec == 2 and date:
            out[uid] = day_of(date * 10000 + 701)
    out.update({uid: day_of(date) for uid, date in registered.items()})
    return out


def uid_int(col: str = "uid") -> pl.Expr:
    return pl.col(col).str.replace_all(r"\D", "").cast(pl.Int64, strict=False)


def legal_letter(col: str = "legalForm") -> pl.Expr:
    return pl.col(col).replace_strict(LEGAL, default="B").fill_null("B").alias("t")


def build_lamps(ev: pl.DataFrame, days: dict, active: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    """One lamp per company. ev: SHAB events placed on a building (uid, kind, date "YYYY-MM-DD",
    name, legalForm, lat, lng); days: uid (int) -> opening day; active: today's companies
    (uid, name, legalForm, lat, lng). Returns the lamps sorted by (o, c) and counts by source."""
    counts = {}
    entry = pl.DataFrame({"u": list(days), "e": list(days.values())},
                         schema={"u": pl.Int64, "e": pl.Int64})
    if "id" not in ev.columns:
        ev = ev.with_row_index("id")
    ev = ev.filter(pl.col("lat").is_not_null()).with_columns(
        (pl.col("date").str.to_date() - pl.lit(EPOCH)).dt.total_days().alias("day"), legal_letter())

    # one lamp per company life: its first opening and the first closing after it. A closing with
    # no opening before it is lit from the register entry date when there is one. Events without
    # a uid can't be paired: kept alone.
    keyed = ev.filter(pl.col("uid") != "")
    opens = keyed.filter(pl.col("kind") == "open").sort("day").unique("uid", keep="first")
    closes = keyed.filter(pl.col("kind") == "close")
    after = (opens.select("uid", pl.col("day").alias("o"))
             .join(closes.select("uid", pl.col("day").alias("c"), pl.col("id").alias("cid")), on="uid")
             .filter(pl.col("c") >= pl.col("o")).sort("c").unique("uid", keep="first"))
    life = opens.join(after.select("uid", "c", "cid"), on="uid", how="left")
    orphan = (closes.join(after.select("cid"), left_on="id", right_on="cid", how="anti")
              .with_columns(uid_int().alias("u")).join(entry, on="u", how="left"))
    late = orphan.filter(pl.col("e") > pl.col("day")).height
    orphan = orphan.with_columns(pl.when(pl.col("e") <= pl.col("day")).then("e").otherwise(NONE).alias("o"))
    counts["shab"] = life.height
    counts["close_only_dated"] = orphan.filter(pl.col("o") != NONE).height
    counts["close_only_undated"] = orphan.filter(pl.col("o") == NONE).height
    counts["open_after_close"] = late
    loose = ev.filter(pl.col("uid") == "")

    # today's companies with no SHAB event at all: lit at their current address
    seen = keyed.select(uid_int().alias("u")).unique()
    act = (active.with_columns(uid_int().alias("u"), legal_letter())
           .join(seen, on="u", how="anti").join(entry, on="u", how="left"))
    counts["active_no_date"] = act.filter(pl.col("e").is_null()).height
    counts["active_no_geo"] = act.filter(pl.col("e").is_not_null() & pl.col("lat").is_null()).height
    act = act.filter(pl.col("e").is_not_null() & pl.col("lat").is_not_null())
    counts["active"] = act.height

    lamps = pl.concat([
        life.select("name", "t", "lat", "lng", pl.col("day").alias("o"), pl.col("c").fill_null(NONE)),
        orphan.select("name", "t", "lat", "lng", "o", pl.col("day").alias("c")),
        loose.select("name", "t", "lat", "lng",
                     pl.when(pl.col("kind") == "open").then("day").otherwise(NONE).alias("o"),
                     pl.when(pl.col("kind") == "close").then("day").otherwise(NONE).alias("c")),
        act.select("name", "t", "lat", "lng", pl.col("e").alias("o"), pl.lit(NONE).alias("c")),
    ], how="vertical_relaxed").with_columns(pl.col("o", "c").cast(pl.Int64)).sort("o", "c")
    return lamps.select(LAMP_COLS), counts


def write_timeline(lamps: pl.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    n = lamps.height
    buf = b"".join([
        struct.pack("<I", n),
        (lamps["lat"] * 1e5).round().cast(pl.Int32).to_numpy().astype("<i4").tobytes(),
        (lamps["lng"] * 1e5).round().cast(pl.Int32).to_numpy().astype("<i4").tobytes(),
        lamps["o"].cast(pl.UInt16).to_numpy().astype("<u2").tobytes(),
        lamps["c"].cast(pl.UInt16).to_numpy().astype("<u2").tobytes(),
        "".join(lamps["t"].to_list()).encode("ascii"),
    ])
    with open(f"{out_dir}/events.bin.gz", "wb") as f:
        f.write(gzip.compress(buf, 9))
    with open(f"{out_dir}/names.json.gz", "wb") as f:
        f.write(gzip.compress(json.dumps(lamps["name"].to_list(), ensure_ascii=False,
                                         separators=(",", ":")).encode(), 9))


def place_events() -> pl.DataFrame:
    """SHAB events geocoded; an event placed only by zip or seat borrows its other event's
    building (a company that moved keeps each event at its own address)."""
    from swisstopo_geo import geocode
    rows = []
    with open(EVENTS, encoding="utf-8") as f:   # line by line: a killed run leaves one broken line
        for line in f:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    ev = pl.DataFrame(rows, infer_schema_length=None).unique("id")
    print(f"[READ] {ev.height:,} events  open {ev.filter(pl.col('kind') == 'open').height:,}  "
          f"close {ev.filter(pl.col('kind') == 'close').height:,}")
    ev = geocode(ev.with_columns(pl.col(c).fill_null("") for c in ["street", "houseNumber", "zip", "town", "seat"]))
    ev.write_parquet("build/timeline_geo.parquet")
    rank = {"building": 0, "street": 1, "zip": 2, "seat": 3, "none": 4}
    ev = ev.with_columns(pl.col("prec").replace_strict(rank).alias("r"))
    best = (ev.filter(pl.col("uid") != "").sort("r")
            .group_by("uid").agg(pl.col("lat", "lng", "r").first().name.prefix("b_")))
    ev = ev.join(best, on="uid", how="left").with_columns(
        borrow=(pl.col("r") >= rank["zip"]) & (pl.col("b_r") < pl.col("r")),
    ).with_columns(
        pl.when("borrow").then(pl.col("b_lat")).otherwise(pl.col("lat")).alias("lat"),
        pl.when("borrow").then(pl.col("b_lng")).otherwise(pl.col("lng")).alias("lng"),
    )
    return ev


def read_columnar(path: str, value: str) -> dict:
    d = json.load(gzip.open(path))
    return dict(zip(d["uid"], d[value] if value != "estimate" else zip(d["date"], d["precision"])))


def main():
    ev = place_events()
    registered = read_columnar(REGISTERED, "date")
    if os.path.exists(ESTIMATES):
        estimates = read_columnar(ESTIMATES, "estimate")
    else:
        print(f"[WARN] {ESTIMATES} not found: no estimated dates")
        estimates = None
    days = entry_days(registered, estimates)
    if os.path.exists(ACTIVE):
        active = pl.read_parquet(ACTIVE, columns=["uid", "name", "legalForm", "lat", "lng"])
    else:   # scripts/active_firms.py not run yet: only the SHAB companies
        print(f"[WARN] {ACTIVE} not found: no lamps for companies without a SHAB event")
        active = pl.DataFrame(schema={"uid": pl.String, "name": pl.String, "legalForm": pl.String,
                                      "lat": pl.Float64, "lng": pl.Float64})
    lamps, counts = build_lamps(ev, days, active)
    print(f"[DATES] register {len(registered):,}  with estimates {len(days):,}")
    print("[LAMPS] " + "  ".join(f"{k} {v:,}" for k, v in counts.items()) + f"  total {lamps.height:,}")
    dec = (lamps.filter(pl.col("o") != NONE)
           .with_columns((((pl.col("o") / 365.25).floor() + EPOCH.year) // 10 * 10).cast(pl.Int32).alias("dec"))
           .group_by("dec").len().sort("dec"))
    print("[DECADES] " + "  ".join(f"{d}s {n:,}" for d, n in dec.iter_rows()))
    write_timeline(lamps, OUT)
    for name in ("events.bin.gz", "names.json.gz"):
        print(f"[OUT] {OUT}/{name}  {os.path.getsize(f'{OUT}/{name}') / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
