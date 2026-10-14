#!/usr/bin/env python3
"""Add each company's entry year to the canton files, for the '98 next to its name.

The canton files (data/{C}_{k}.json.gz, columns n, t, lat, lng) carry no uid, so a
company is found in LINDAS (build/lindas_active.parquet) by name and canton, else by
name alone when every company of that name shares one year. Its year comes from, in
order: the cantonal register (data/registered.json.gz), its SHAB new entry
(build/shab_events.jsonl), the estimate of opening_dates.json.gz when it gives the day
or the year (precision 0-2). Adds column y (the year, 0 = unknown); a re-run replaces it.

Usage:
    python3 scripts/canton_years.py
"""
import glob, gzip, json, os, re

import polars as pl

ACTIVE = "build/lindas_active.parquet"
REGISTERED = "data/registered.json.gz"
EVENTS = "build/shab_events.jsonl"
ESTIMATES = os.path.expanduser(os.environ.get("OPENING_ESTIMATES", "~/Projetos/swissviz/data/opening_dates.json.gz"))


def uid_int(s: str) -> int | None:
    d = re.sub(r"\D", "", s or "")
    return int(d) if d else None


def years() -> dict:
    """uid (int) -> entry year, the most reliable source last so it wins."""
    out = {}
    if os.path.exists(ESTIMATES):
        e = json.load(gzip.open(ESTIMATES))
        out.update((u, d // 10000) for u, d, p in zip(e["uid"], e["date"], e["precision"]) if p <= 2 and d)
    with open(EVENTS, encoding="utf-8") as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            u = uid_int(r.get("uid"))
            if r.get("kind") == "open" and u and r.get("date"):
                out[u] = int(r["date"][:4])
    r = json.load(gzip.open(REGISTERED))
    out.update((u, d // 10000) for u, d in zip(r["uid"], r["date"]))
    return out


def main():
    year = years()
    act = (pl.read_parquet(ACTIVE, columns=["uid", "name", "canton"])
           .with_columns(pl.col("uid").map_elements(uid_int, return_dtype=pl.Int64).alias("u"))
           .with_columns(pl.col("u").replace_strict(year, default=0, return_dtype=pl.Int32).alias("y")))
    by_nc = act.group_by("name", "canton").agg(pl.col("y").first().alias("y_nc"), pl.len().alias("k_nc"))
    by_n = act.group_by("name").agg(pl.col("y").first().alias("y_n"), pl.col("y").n_unique().alias("ny"))
    total = known = 0
    for path in sorted(glob.glob("data/[A-Z][A-Z]_*.json.gz")):
        d = json.load(gzip.open(path))
        df = (pl.DataFrame({"n": d["n"]}).with_columns(canton=pl.lit(os.path.basename(path)[:2]))
              .join(by_nc, left_on=["n", "canton"], right_on=["name", "canton"], how="left", maintain_order="left")
              .join(by_n, left_on="n", right_on="name", how="left", maintain_order="left"))
        y = df.select(pl.when(pl.col("k_nc") == 1).then("y_nc")
                      .when(pl.col("ny") == 1).then("y_n")
                      .otherwise(0).fill_null(0)).to_series().to_list()
        d["y"] = y
        with open(path, "wb") as f:
            f.write(gzip.compress(json.dumps(d, separators=(",", ":"), ensure_ascii=False).encode(), 9))
        total += len(y)
        known += sum(1 for v in y if v)
    print(f"[YEARS] {known:,} of {total:,} companies ({known / total:.1%})")


if __name__ == "__main__":
    main()
