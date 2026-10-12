#!/usr/bin/env python3
"""Turn build/shab_events.jsonl into the map's timeline file.

Each company becomes one lamp with the day it opened and the day it closed
(days since 2018-01-01; 65535 = not in the window), placed on its building by
the swisstopo address register.

Output: data/timeline/events.bin.gz, little-endian, all columns n long:
    uint32 n
    int32  lat * 1e5      int32 lng * 1e5
    uint16 open day       uint16 close day
    uint8  legal form (ASCII letter, see T_DECODE in index.html)
plus data/timeline/names.json.gz (names in the same order, loaded on click)
and build/timeline_geo.parquet (every event with its precision, for checks).

Usage:
    python3 scripts/build_timeline.py
"""
import datetime as dt, gzip, json, os, struct, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from swisstopo_geo import geocode

EVENTS = os.environ.get("SHAB_EVENTS", "build/shab_events.jsonl")
OUT = "data/timeline"
EPOCH = dt.date(2018, 1, 1)
NONE = 65535

# eCH-0097 legal form → one-letter code used by the site (scripts/repack.py)
LEGAL = {
    "0101": "E", "0103": "K", "0104": "O", "0105": "B", "0106": "A", "0107": "G",
    "0108": "N", "0109": "V", "0110": "S", "0111": "L", "0113": "B", "0114": "B",
    "0115": "B", "0116": "B", "0117": "I", "0118": "B", "0119": "H", "0151": "Z",
}

# line by line: a run killed mid-write leaves one broken line
rows = []
with open(EVENTS, encoding="utf-8") as f:
    for line in f:
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass
ev = pl.DataFrame(rows, infer_schema_length=None).unique("id")
print(f"[READ] {ev.height:,} events  "
      f"open {ev.filter(pl.col('kind') == 'open').height:,}  "
      f"close {ev.filter(pl.col('kind') == 'close').height:,}")

unmapped = ev.filter(~pl.col("legalForm").is_in(list(LEGAL)))["legalForm"].value_counts()
if unmapped.height:
    print("[WARN] unmapped legal forms → B:", unmapped.to_dicts())

ev = geocode(ev.with_columns(pl.col(c).fill_null("") for c in
                             ["street", "houseNumber", "zip", "town", "seat"]))
ev.write_parquet("build/timeline_geo.parquet")

# per company: an event placed only by zip or seat borrows its other event's
# building (a company that moved keeps each event at its own address)
rank = {"building": 0, "street": 1, "zip": 2, "seat": 3, "none": 4}
ev = ev.with_columns(pl.col("prec").replace_strict(rank).alias("r"))
best = (ev.filter(pl.col("uid") != "").sort("r")
        .group_by("uid").agg(pl.col("lat", "lng", "r").first().name.prefix("b_")))
ev = ev.join(best, on="uid", how="left").with_columns(
    borrow=(pl.col("r") >= rank["zip"]) & (pl.col("b_r") < pl.col("r")),
).with_columns(
    pl.when("borrow").then(pl.col("b_lat")).otherwise(pl.col("lat")).alias("lat"),
    pl.when("borrow").then(pl.col("b_lng")).otherwise(pl.col("lng")).alias("lng"),
    pl.when("borrow").then(pl.col("b_r")).otherwise(pl.col("r")).alias("r"),
)

print("[GEO] precision by event:")
names = {v: k for k, v in rank.items()}
for row in ev.group_by("r").len().sort("r").iter_rows():
    print(f"   {names[row[0]]:9} {row[1]:8,}  {100 * row[1] / ev.height:5.1f}%")

ev = ev.filter(pl.col("lat").is_not_null()).with_columns(
    ((pl.col("date").str.to_date() - pl.lit(EPOCH)).dt.total_days()).alias("day"),
    pl.col("legalForm").replace_strict(LEGAL, default="B").alias("t"),
)
early = ev.filter(pl.col("day") < 0).height
if early:
    print(f"[WARN] {early:,} events dated before {EPOCH}, dropped")
ev = ev.filter(pl.col("day") >= 0)

# one lamp per company life: its first opening and the first closing after it.
# A closing with no opening before it (company older than the window) is a
# lamp that only goes out. Events without a uid can't be paired: kept alone.
keyed = ev.filter(pl.col("uid") != "")
opens = keyed.filter(pl.col("kind") == "open").sort("day").unique("uid", keep="first")
closes = keyed.filter(pl.col("kind") == "close")
after = (opens.select("uid", pl.col("day").alias("o"))
         .join(closes.select("uid", pl.col("day").alias("c"), pl.col("id").alias("cid")), on="uid")
         .filter(pl.col("c") >= pl.col("o")).sort("c").unique("uid", keep="first"))
life = opens.join(after.select("uid", "c", "cid"), on="uid", how="left")
orphan = closes.join(after.select("cid"), left_on="id", right_on="cid", how="anti")
loose = ev.filter(pl.col("uid") == "")
lamps = pl.concat([
    life.select("name", "t", "lat", "lng", pl.col("day").alias("o"), pl.col("c").fill_null(NONE)),
    orphan.select("name", "t", "lat", "lng", pl.lit(NONE).alias("o"), pl.col("day").alias("c")),
    loose.select("name", "t", "lat", "lng",
                 pl.when(pl.col("kind") == "open").then("day").otherwise(NONE).alias("o"),
                 pl.when(pl.col("kind") == "close").then("day").otherwise(NONE).alias("c")),
], how="vertical_relaxed").sort("o", "c")
print(f"[LAMPS] {lamps.height:,}  open→close {lamps.filter((pl.col('o') != NONE) & (pl.col('c') != NONE)).height:,}  "
      f"open only {lamps.filter(pl.col('c') == NONE).height:,}  "
      f"close only {lamps.filter(pl.col('o') == NONE).height:,}  (no uid {loose.height:,})")

os.makedirs(OUT, exist_ok=True)
n = lamps.height
buf = b"".join([
    struct.pack("<I", n),
    (lamps["lat"] * 1e5).round().cast(pl.Int32).to_numpy().astype("<i4").tobytes(),
    (lamps["lng"] * 1e5).round().cast(pl.Int32).to_numpy().astype("<i4").tobytes(),
    lamps["o"].cast(pl.UInt16).to_numpy().astype("<u2").tobytes(),
    lamps["c"].cast(pl.UInt16).to_numpy().astype("<u2").tobytes(),
    "".join(lamps["t"].to_list()).encode("ascii"),
])
with open(f"{OUT}/events.bin.gz", "wb") as f:
    f.write(gzip.compress(buf, 9))
with open(f"{OUT}/names.json.gz", "wb") as f:
    f.write(gzip.compress(json.dumps(lamps["name"].to_list(), ensure_ascii=False,
                                     separators=(",", ":")).encode(), 9))
for name in ("events.bin.gz", "names.json.gz"):
    print(f"[OUT] {OUT}/{name}  {os.path.getsize(f'{OUT}/{name}') / 1e6:.1f} MB")
