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

Output: data/timeline/events.bin.gz, little-endian (decoded by loadTimeline in index.html):
    uint32 n               uint32 names per chunk
    float64 lat0, lng0     float64 lat step, lng step (uint16 grid over the lamps' box, ~3-5 m)
then five columns, n long each, every multi-byte column split into its low-byte plane
followed by its high-byte plane (gzip packs the planes much better than the interleaved bytes):
    uint16 lat cell, uint16 lng cell: zigzag delta from the previous lamp, modulo 2^16
    uint16 open day        uint16 close day
    uint8  legal form (ASCII letter, see T_DECODE in index.html)
The lamps are sorted along a Morton curve, so neighbours on the map are neighbours in the
file: the coordinate deltas stay small, and each names chunk covers one region.
Plus data/timeline/names/{k}.json.gz (the names of lamps k*chunk .. (k+1)*chunk-1, fetched
on the first hover over one of them)
and build/timeline_geo.parquet (every SHAB event with its precision, for checks).

Usage:
    python3 scripts/build_timeline.py
"""
import datetime as dt, glob, gzip, json, os, struct, sys

import numpy as np

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))

EVENTS = os.environ.get("SHAB_EVENTS", "build/shab_events.jsonl")
REGISTERED = "data/registered.json.gz"
ACTIVE = "build/lindas_active.parquet"
ESTIMATES = os.environ.get("OPENING_ESTIMATES", "data/opening_dates.json.gz")
OUT = "data/timeline"
EPOCH = dt.date(1883, 1, 1)
NONE = 65535
NAMES_CHUNK = 16384
# LINDAS lags the SHAB by a few weeks: a company entered this close to the snapshot may just be missing
GRACE_DAYS = 30

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


def norm_uid(col: str = "uid") -> pl.Expr:
    """The SHAB writes some UIDs bare (CHE492310367): one spelling, CHE-123.456.789, for pairing."""
    d = pl.col(col).fill_null("").str.replace_all(r"\D", "")
    return (pl.when(d.str.len_chars() == 9)
            .then(pl.format("CHE-{}.{}.{}", d.str.slice(0, 3), d.str.slice(3, 3), d.str.slice(6, 3)))
            .otherwise(pl.col(col).fill_null("")).alias(col))


def legal_letter(col: str = "legalForm") -> pl.Expr:
    return pl.col(col).replace_strict(LEGAL, default="B").fill_null("B").alias("t")


def build_lamps(ev: pl.DataFrame, days: dict, active: pl.DataFrame,
                snapshot_day: int | None = None) -> tuple[pl.DataFrame, dict]:
    """One lamp per company. ev: SHAB events placed on a building (uid, kind, date "YYYY-MM-DD",
    name, legalForm, lat, lng); days: uid (int) -> opening day; active: today's companies
    (uid, name, legalForm, lat, lng); snapshot_day: the day of that list. Returns the lamps
    sorted by (o, c) and counts by source."""
    counts = {}
    entry = pl.DataFrame({"u": list(days), "e": list(days.values())},
                         schema={"u": pl.Int64, "e": pl.Int64})
    if "id" not in ev.columns:
        ev = ev.with_row_index("id")
    ev = ev.with_columns(norm_uid())
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
    # a company alive today shines at its current address under its current name (LINDAS), so the
    # end of the timeline falls on the map's points; one that left the register with no SHAB
    # deletion (merger, transfer…) goes dark on the day of the LINDAS list
    cur = active.select(uid_int().alias("u"), pl.col("name").alias("cur_name"),
                        pl.col("lat").alias("cur_lat"), pl.col("lng").alias("cur_lng"))
    life = life.with_columns(uid_int().alias("u")).join(cur, on="u", how="left")
    now = pl.col("c").is_null() & pl.col("cur_lat").is_not_null()
    counts["moved_to_current"] = life.filter(now & ((pl.col("lat") != pl.col("cur_lat")) |
                                                   (pl.col("lng") != pl.col("cur_lng")))).height
    life = life.with_columns(pl.when(now).then(pl.col(f"cur_{k}")).otherwise(pl.col(k)).alias(k)
                             for k in ("name", "lat", "lng"))
    if snapshot_day is not None and active.height:
        gone = (pl.col("c").is_null() & pl.col("cur_lat").is_null()
                & (pl.col("day") < snapshot_day - GRACE_DAYS))
        counts["left_without_deletion"] = life.filter(gone).height
        life = life.with_columns(pl.when(gone).then(pl.lit(snapshot_day)).otherwise(pl.col("c")).alias("c"))
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


def morton(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """Interleave the bits of two uint16 arrays: a key that keeps nearby cells close."""
    x, y, r = x.astype(np.uint64), y.astype(np.uint64), np.zeros(len(x), np.uint64)
    for b in range(16):
        r |= ((x >> np.uint64(b)) & np.uint64(1)) << np.uint64(2 * b)
        r |= ((y >> np.uint64(b)) & np.uint64(1)) << np.uint64(2 * b + 1)
    return r


def planes(a: np.ndarray) -> bytes:
    """Low bytes of every value, then high bytes."""
    return a.astype("<u2").view(np.uint8).reshape(-1, 2).T.tobytes()


def zigzag_delta(a: np.ndarray) -> np.ndarray:
    d = np.diff(a.astype(np.int64), prepend=0).astype(np.uint16).view(np.int16).astype(np.int32)
    return ((d << 1) ^ (d >> 15)).astype(np.uint16)


def write_timeline(lamps: pl.DataFrame, out_dir: str):
    os.makedirs(f"{out_dir}/names", exist_ok=True)
    lat, lng = lamps["lat"].to_numpy(), lamps["lng"].to_numpy()
    lat0, lng0 = float(lat.min()), float(lng.min())
    lat_step, lng_step = (float(lat.max()) - lat0) / 65535, (float(lng.max()) - lng0) / 65535
    qa = np.round((lat - lat0) / lat_step).astype(np.uint16)
    qn = np.round((lng - lng0) / lng_step).astype(np.uint16)
    order = np.lexsort((lamps["c"].to_numpy(), lamps["o"].to_numpy(), morton(qn, qa)))
    lamps, qa, qn = lamps[order], qa[order], qn[order]
    n = lamps.height
    buf = b"".join([
        struct.pack("<II4d", n, NAMES_CHUNK, lat0, lng0, lat_step, lng_step),
        planes(zigzag_delta(qa)), planes(zigzag_delta(qn)),
        planes(lamps["o"].to_numpy()), planes(lamps["c"].to_numpy()),
        "".join(lamps["t"].to_list()).encode("ascii"),
    ])
    with open(f"{out_dir}/events.bin.gz", "wb") as f:
        f.write(gzip.compress(buf, 9))
    for old in glob.glob(f"{out_dir}/names/*.json.gz") + glob.glob(f"{out_dir}/names.json.gz"):
        os.remove(old)
    names = lamps["name"].to_list()
    for k in range(0, n, NAMES_CHUNK):
        with open(f"{out_dir}/names/{k // NAMES_CHUNK}.json.gz", "wb") as f:
            f.write(gzip.compress(json.dumps(names[k:k + NAMES_CHUNK], ensure_ascii=False,
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
    ev = pl.DataFrame(rows, infer_schema_length=None).unique("id").with_columns(norm_uid())
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
    snapshot_day = None
    if os.path.exists(ACTIVE):
        snapshot_day = (dt.date.fromtimestamp(os.path.getmtime(ACTIVE)) - EPOCH).days
    lamps, counts = build_lamps(ev, days, active, snapshot_day)
    print(f"[DATES] register {len(registered):,}  with estimates {len(days):,}")
    print("[LAMPS] " + "  ".join(f"{k} {v:,}" for k, v in counts.items()) + f"  total {lamps.height:,}")
    dec = (lamps.filter(pl.col("o") != NONE)
           .with_columns((((pl.col("o") / 365.25).floor() + EPOCH.year) // 10 * 10).cast(pl.Int32).alias("dec"))
           .group_by("dec").len().sort("dec"))
    print("[DECADES] " + "  ".join(f"{d}s {n:,}" for d, n in dec.iter_rows()))
    write_timeline(lamps, OUT)
    print(f"[OUT] {OUT}/events.bin.gz  {os.path.getsize(f'{OUT}/events.bin.gz') / 1e6:.1f} MB")
    chunks = [os.path.getsize(f) for f in glob.glob(f"{OUT}/names/*.json.gz")]
    print(f"[OUT] {OUT}/names/  {len(chunks)} chunks  {sum(chunks) / 1e6:.1f} MB  "
          f"largest {max(chunks) / 1e3:.0f} kB")


if __name__ == "__main__":
    main()
