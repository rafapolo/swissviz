#!/usr/bin/env python3
"""Write the map's canton files and search index from today's companies in LINDAS.

The map and the timeline share one source for "today": build/lindas_active.parquet
(scripts/active_firms.py: current name, legal form and swisstopo coordinates), so at the
end of the timeline every lit lamp falls on a point of the map. See tasks/cobertura-dados.md.

Output:
    data/{C}_0.json.gz   columns n (name), t (legal form letter), lat, lng (5 decimals),
                         y (entry year, 0 = unknown: cantonal register, else SHAB
                         new entry, else an estimate of precision 0-2)
    data/{C}.json        {"_chunks": 1}
    data/search_index.json.gz   columns n, t, c (canton), lat, lng (2 decimals)
Then run scripts/canton_counts.py for the totals in index.html.

Usage:
    python3 scripts/canton_files.py
"""
import gzip, json, os, re, sys

import polars as pl

sys.path.insert(0, os.path.dirname(__file__))
from build_timeline import LEGAL

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


def dump(obj, path):
    with open(path, "wb") as f:
        f.write(gzip.compress(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode(), 9))


def main():
    year = years()
    act = (pl.read_parquet(ACTIVE, columns=["uid", "name", "legalForm", "canton", "lat", "lng"])
           .filter(pl.col("lat").is_not_null() & pl.col("lng").is_not_null() & pl.col("canton").is_not_null())
           .with_columns(pl.col("legalForm").replace_strict(LEGAL, default="B").fill_null("B").alias("t"),
                         pl.col("uid").map_elements(uid_int, return_dtype=pl.Int64).alias("u"),
                         pl.col("name").fill_null(""))
           .with_columns(pl.col("u").replace_strict(year, default=0, return_dtype=pl.Int32).alias("y"))
           .sort("canton", "name"))
    for (canton,), g in act.group_by("canton", maintain_order=True):
        dump({"n": g["name"].to_list(), "t": g["t"].to_list(),
              "lat": g["lat"].round(5).to_list(), "lng": g["lng"].round(5).to_list(),
              "y": g["y"].to_list()}, f"data/{canton}_0.json.gz")
        with open(f"data/{canton}.json", "w") as f:
            json.dump({"_chunks": 1}, f)
    dump({"n": act["name"].to_list(), "t": act["t"].to_list(), "c": act["canton"].to_list(),
          "lat": act["lat"].round(2).to_list(), "lng": act["lng"].round(2).to_list()},
         "data/search_index.json.gz")
    known = (act["y"] > 0).sum()
    print(f"[CANTONS] {act['canton'].n_unique()} files  {act.height:,} companies  "
          f"with a year {known:,} ({known / act.height:.2%})")


if __name__ == "__main__":
    main()
