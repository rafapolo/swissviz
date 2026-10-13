#!/usr/bin/env python3
"""List the SHAB archive (2002 – 2 Sep 2018): every new entry and deletion.

The archive predates the structured API: each notice has only a title
("NAME, SEAT"), its date and a PDF of its printed page. The titles are enough
to date the companies we already have, so this keeps the list only, in
build/shab_archive.parquet (id, sub = hr01 | hr03, date, title).

Usage:
    python3 scripts/fetch_shab_archive.py
"""
import datetime as dt, gzip, json, os, random, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

import polars as pl

API = "https://amtsblattportal.ch/api/v1/archive/public"
OUT = "build/shab_archive.parquet"
START, END = dt.date(2001, 1, 1), dt.date(2018, 9, 1)


def get(url, tries=6):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                       "User-Agent": "cantography/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read()
            return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
        except Exception as e:
            err = e
            time.sleep(min(60, 2 ** i) + random.random())
    raise RuntimeError(f"{url}: {err}")


def window(sub, first, last):
    """One page of up to 3000; a window that holds more is split in two."""
    d = get(f"{API}?tenant=shab&subRubrics={sub}&publicationDate.start={first}"
            f"&publicationDate.end={last}&pageRequest.size=3000&pageRequest.page=0")
    # a window with nothing in it comes back as the whole archive, unfiltered
    if d["content"] and not first.isoformat() <= d["content"][0]["publicationTime"][:10] <= last.isoformat():
        return []
    if d["total"] > 3000:
        if first == last:
            raise RuntimeError(f"{sub} {first}: {d['total']} in one day")
        mid = first + (last - first) // 2
        return window(sub, first, mid) + window(sub, mid + dt.timedelta(days=1), last)
    if len(d["content"]) != d["total"]:
        print(f"  [WARN] {sub} {first}..{last}: {len(d['content'])} of {d['total']}", flush=True)
    return [{"id": c["id"], "sub": sub, "date": c["publicationTime"][:10], "title": c["title"]}
            for c in d["content"]]


def months():
    y, m = START.year, START.month
    while dt.date(y, m, 1) <= END:
        first = dt.date(y, m, 1)
        nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
        yield first, min(END, nxt - dt.timedelta(days=1))
        y, m = nxt.year, nxt.month


jobs = [(sub, a, b) for a, b in months() for sub in ("hr01", "hr03")]
rows = []
# the archive backend is fragile: 8 threads made it stop answering
with ThreadPoolExecutor(3) as ex:
    for (sub, a, _), r in zip(jobs, ex.map(lambda j: window(*j), jobs)):
        if a.month == 1:
            print(f"[ARCHIVE] {a.year} {sub} …", flush=True)
        rows += r
df = pl.DataFrame(rows)
dups = df.height - df["id"].n_unique()
df = df.unique("id", keep="first")
os.makedirs("build", exist_ok=True)
df.write_parquet(OUT)
print(f"[ARCHIVE] {df.height:,} notices ({dups:,} duplicates dropped) -> {OUT}")
print(df.with_columns(pl.col("date").str.slice(0, 4).alias("y"))
      .group_by("y", "sub").len().pivot(on="sub", index="y", values="len").sort("y"))
