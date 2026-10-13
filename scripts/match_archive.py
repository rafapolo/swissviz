#!/usr/bin/env python3
"""Date the companies we have by their new entry in the SHAB archive (2002-2018).

The archive has no uid, only a title "NAME, SEAT", so companies are matched
on normalized name + seat. A name gains "in Liquidation" before it is deleted,
so that suffix is dropped on both sides. Names used by more than one archive
entry in the same seat are ambiguous and left out.

Output: build/archive_opens.parquet (uid, open_date, source = archive)
"""
import os, re, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from swisstopo_geo import norm_place

LIQ = re.compile(r"\s*[,(]?\s*(in liquidation|en liquidation|in liquidazione|in liq\.?|en liq\.?)\)?\s*$", re.I)


def norm_name(s: str) -> str:
    s = LIQ.sub("", s or "")
    return re.sub(r"[^0-9a-zà-ÿ]+", "", s.casefold())


arc = pl.read_parquet("build/shab_archive.parquet").filter(pl.col("sub") == "hr01")
# the seat is after the last comma (names can hold commas)
arc = arc.with_columns(
    pl.col("title").str.replace(r",\s*[^,]*$", "").alias("name"),
    pl.col("title").str.extract(r",\s*([^,]*)$").alias("seat"),
)
arc = arc.with_columns(
    pl.col("name").map_elements(norm_name, return_dtype=pl.Utf8).alias("nk"),
    pl.col("seat").fill_null("").map_elements(norm_place, return_dtype=pl.Utf8).alias("sk"),
)
dup = arc.group_by("nk", "sk").len().filter(pl.col("len") > 1)
arc_u = arc.join(dup, on=["nk", "sk"], how="anti")

ev = pl.read_parquet("build/timeline_geo.parquet")
comp = (ev.filter(pl.col("uid") != "").sort("date")
        .group_by("uid").agg(pl.col("name", "seat", "kind").first(),
                             (pl.col("kind") == "open").any().alias("has_open")))
comp = comp.with_columns(
    pl.col("name").map_elements(norm_name, return_dtype=pl.Utf8).alias("nk"),
    pl.col("seat").map_elements(norm_place, return_dtype=pl.Utf8).alias("sk"),
)
need = comp.filter(~pl.col("has_open"))
hit = need.join(arc_u.select("nk", "sk", pl.col("date").alias("open_date")), on=["nk", "sk"])
# a name kept but seat moved: match on the name alone when it is unique overall
uniq = arc.group_by("nk").len().filter(pl.col("len") == 1).select("nk")
rest = need.join(hit.select("uid"), on="uid", how="anti")
hit2 = rest.join(arc.join(uniq, on="nk").select("nk", pl.col("date").alias("open_date")), on="nk")
out = pl.concat([hit.select("uid", "open_date", pl.lit("name+seat").alias("how")),
                 hit2.select("uid", "open_date", pl.lit("name").alias("how"))])
out.write_parquet("build/archive_opens.parquet")
print(f"companies closed 2018-2026 without an opening since 2018: {need.height:,}")
print(f"  dated by the archive: {out.height:,} ({100 * out.height / need.height:.1f}%)  "
      f"name+seat {hit.height:,}, name only {hit2.height:,}")
print("  the rest opened before 2002 (archive starts 2002) or renamed")
print(out.with_columns(pl.col("open_date").str.slice(0, 4).alias("y")).group_by("y").len().sort("y"))

# false-positive check: companies that opened after the archive ended can't be
# in it, so any match among them is a wrong match (a namesake)
known = comp.filter(pl.col("has_open"))
fp1 = known.join(arc_u.select("nk", "sk"), on=["nk", "sk"]).height
fp2 = known.join(arc.join(uniq, on="nk").select("nk"), on="nk").height
print(f"false-positive check on {known.height:,} companies opened since Sep 2018: "
      f"name+seat {fp1:,} ({100 * fp1 / known.height:.2f}%), name only {fp2:,} ({100 * fp2 / known.height:.2f}%)")
