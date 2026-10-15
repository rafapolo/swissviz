#!/usr/bin/env python3
"""Opening (commercial register entry) date of every existing Swiss company.

No public source has the entry date of every company, so it is put together:

  1. LINDAS (Zefix as linked data): uid and ehraid of every existing company.
  2. SHAB "Neueintragung" publications (mid-2018 on, read from the timeline
     fetch, see tasks/timeline-shab.md): the exact entry date, by uid.
  3. The ehraid is handed out in sequence when a company is entered, from
     about 1999 on. Anchors with a known date place the rest on that line:
       - zefix.ch firm details: the "status.neu" publication of a sample of
         companies from mid-2016 on (the history there starts in June 2016);
       - Wikidata inception (P571) of companies with a Swiss UID (P4829),
         read as an upper bound per ehraid bin, for 1999-2016.
     Below EHRAID_ORDERED the ehraids come from the bulk load of the central
     register and say nothing about the year: those are "before ~1999".

Output, columnar and gzipped: data/opening_dates.json.gz
  {"asOf": "YYYY-MM-DD", "uid": [123456789, ...], "date": [20190314, 2007, 0, ...],
   "precision": [0, 1, 2, 3, ...]}
  precision 0 exact day (SHAB), 1 day interpolated from ehraid,
            2 year estimated from ehraid, 3 before ~1999 (date 0)

Usage: python3 scripts/opening_dates.py [--anchors 300] [--shab PATH]
Caches in build/ (LINDAS ids, zefix anchors, Wikidata anchors).
"""
import argparse, datetime, gzip, io, json, os, sys, time, urllib.parse, urllib.request
import polars as pl

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD = os.path.join(ROOT, "build")
OUT = os.path.join(ROOT, "data", "opening_dates.json.gz")
UA = "cantography/0.1 (https://github.com/rafapolo/cantography)"
# ehraids below this come from the bulk load: no order by year
EHRAID_ORDERED = 550_000

p = argparse.ArgumentParser()
p.add_argument("--anchors", type=int, default=300, help="zefix firm details sampled from mid-2016 on")
p.add_argument("--shab", default="build/shab_events.jsonl",
               help="SHAB events JSONL from the timeline fetch (read only)")
args = p.parse_args()
os.makedirs(BUILD, exist_ok=True)


def fetch(url, data=None, headers=None, timeout=120, tries=5):
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, data=data, headers={"User-Agent": UA, **(headers or {})})
            return urllib.request.urlopen(req, timeout=timeout).read()
        except Exception as e:
            print(f"  retry {attempt + 1}: {e}", file=sys.stderr)
            time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"failed: {url}")


# ── 1. LINDAS ids ─────────────────────────────────────────────────────────────
def lindas_ids():
    path = os.path.join(BUILD, "lindas_ids.parquet")
    if os.path.exists(path):
        return pl.read_parquet(path)
    q = """SELECT ?c ?uid WHERE { GRAPH <https://lindas.admin.ch/foj/zefix> {
 ?c a <https://schema.ld.admin.ch/ZefixOrganisation> ; <http://schema.org/identifier> ?i .
 ?i <http://schema.org/name> "CompanyUID" ; <http://schema.org/value> ?uid } }
ORDER BY ?c LIMIT %d OFFSET %d"""
    page, off, parts = 100_000, 0, []
    while True:
        body = urllib.parse.urlencode({"query": q % (page, off)}).encode()
        df = pl.read_csv(io.BytesIO(fetch("https://ld.admin.ch/query", body, {"Accept": "text/csv"}, timeout=300)))
        print(f"  LINDAS offset {off}: {df.height}")
        if df.height:
            parts.append(df)
        if df.height < page:
            break
        off += page
    df = (pl.concat(parts)
          .select(pl.col("c").str.extract(r"/(\d+)$").cast(pl.Int64).alias("ehraid"),
                  pl.col("uid").str.replace_all(r"\D", "").cast(pl.Int64).alias("uid"))
          .unique("ehraid"))
    df.write_parquet(path)
    return df


# ── 2. zefix.ch anchors: "status.neu" of a sample from mid-2016 on ────────────
def zefix_anchors(ids, n):
    path = os.path.join(BUILD, "zefix_anchors.jsonl")
    done = {}
    if os.path.exists(path):
        for line in open(path):
            r = json.loads(line)
            done[r["ehraid"]] = r
    pool = ids.filter(pl.col("ehraid") >= 1_250_000).sort("ehraid")["ehraid"].to_list()
    step = max(1, len(pool) // n)
    todo = [e for e in pool[::step] if e not in done]
    with open(path, "a") as fh:
        for i, e in enumerate(todo):
            try:
                d = json.loads(fetch(f"https://www.zefix.ch/ZefixREST/api/v1/firm/{e}.json",
                                     headers={"Accept": "application/json"}, timeout=30))
            except RuntimeError:
                continue
            date = None
            for pub in d.get("shabPub") or []:
                kinds = pub.get("mutationTypes") or []
                if isinstance(kinds, str):
                    kinds = json.loads(kinds.replace("'", '"'))
                if any(k.get("key") == "status.neu" for k in kinds):
                    date = pub.get("registryOfficeJournalDate") or pub.get("shabDate")
                    break
            r = {"ehraid": e, "date": date}
            done[e] = r
            fh.write(json.dumps(r) + "\n")
            fh.flush()
            if i % 25 == 0:
                print(f"  zefix anchors {i}/{len(todo)}")
            time.sleep(1)  # an undocumented endpoint: stay gentle
    rows = [r for r in done.values() if r["date"]]
    return pl.DataFrame(rows).select(
        "ehraid", pl.col("date").str.to_date().alias("date")) if rows else pl.DataFrame()


# ── 3. Wikidata anchors: inception of companies with a Swiss UID ──────────────
def wikidata_anchors(ids):
    path = os.path.join(BUILD, "wikidata_inception.csv")
    if not os.path.exists(path):
        q = "SELECT ?uid ?inc WHERE { ?x wdt:P4829 ?uid ; wdt:P571 ?inc . }"
        url = "https://query.wikidata.org/sparql?" + urllib.parse.urlencode({"query": q})
        open(path, "wb").write(fetch(url, headers={"Accept": "text/csv"}))
    wd = pl.read_csv(path).select(
        pl.col("uid").str.replace_all(r"\D", "").cast(pl.Int64, strict=False).alias("uid"),
        pl.col("inc").str.slice(0, 4).cast(pl.Int32, strict=False).alias("year"))
    return ids.join(wd.drop_nulls(), on="uid").unique("ehraid")


# ── 4. Exact dates from SHAB (timeline fetch), by uid ─────────────────────────
def shab_exact(path):
    if not os.path.exists(path):
        print(f"  no SHAB events at {path}: exact dates skipped")
        return pl.DataFrame(schema={"uid": pl.Int64, "date": pl.Date})
    rows = []
    with open(path) as fh:  # appended while the fetch runs: skip a torn last line
        for line in fh:
            try:
                r = json.loads(line)
            except json.JSONDecodeError:
                continue
            if r.get("kind") == "open" and r.get("uid") and r.get("date"):
                rows.append((r["uid"], r["date"]))
    df = pl.DataFrame(rows, schema=["uid", "date"], orient="row").select(
        pl.col("uid").str.replace_all(r"\D", "").cast(pl.Int64, strict=False),
        pl.col("date").str.slice(0, 10).str.to_date(strict=False))
    # a re-registration publishes again: the first entry is the opening
    return df.drop_nulls().group_by("uid").agg(pl.col("date").min())


def isotonic(y, w):
    """Weighted pool-adjacent-violators: the closest non-decreasing sequence."""
    blocks = []  # [mean, weight, count]
    for v, k in zip(y, w):
        blocks.append([v, k, 1])
        while len(blocks) > 1 and blocks[-2][0] > blocks[-1][0]:
            m2, w2, c2 = blocks.pop()
            m1, w1, c1 = blocks.pop()
            blocks.append([(m1 * w1 + m2 * w2) / (w1 + w2), w1 + w2, c1 + c2])
    out = []
    for m, _, c in blocks:
        out += [m] * c
    return out


def main():
    ids = lindas_ids()
    print(f"companies: {ids.height}")
    za = zefix_anchors(ids, args.anchors)
    wa = wikidata_anchors(ids)
    exact = shab_exact(args.shab)
    print(f"anchors: zefix {za.height}, wikidata {wa.height}; SHAB exact {exact.height}")

    # Day anchors (mid-2016 on): zefix sample plus SHAB exact dates, ehraid → day
    day = za.with_columns(pl.col("date").cast(pl.Int32).alias("t"))
    if exact.height:
        ex = ids.join(exact, on="uid").select("ehraid", pl.col("date").cast(pl.Int32).alias("t"))
        day = pl.concat([day.select("ehraid", "t"), ex])
    day = day.select("ehraid", "t").sort("ehraid")
    # isotonic by running max: the line only goes forward in time
    day = day.with_columns(pl.col("t").cum_max())
    day_start = day["ehraid"].min() if day.height else None

    # Year anchors (1999-2016): Wikidata inception, upper part of each bin (a
    # company can be founded long before it is entered, rarely after), joined
    # to the day anchors as fractional years and made monotonic in ehraid
    wy = (wa.filter(pl.col("ehraid") >= EHRAID_ORDERED)
          .with_columns((pl.col("ehraid") // 50_000).alias("bin"))
          .group_by("bin").agg(pl.col("ehraid").median().alias("ehraid"),
                               pl.col("year").quantile(0.9).cast(pl.Float64).alias("year"), pl.len())
          .filter(pl.col("len") >= 5)
          .select("ehraid", "year", pl.col("len").cast(pl.Float64).alias("w")))
    if day_start is not None:
        wy = wy.filter(pl.col("ehraid") < day_start)
        dy = day.select(pl.col("ehraid").cast(pl.Float64),
                        (pl.col("t") / 365.2425 + 1970).alias("year"), pl.lit(5.0).alias("w"))
        wy = pl.concat([wy.with_columns(pl.col("ehraid").cast(pl.Float64)), dy])
    yr = wy.sort("ehraid")
    yr = yr.with_columns(pl.Series("year", isotonic(yr["year"].to_list(), yr["w"].to_list())))

    out = ids.sort("ehraid").join(exact, on="uid", how="left")
    e = out["ehraid"].to_numpy()
    import numpy as np
    date = np.zeros(len(e), dtype=np.int64)
    prec = np.full(len(e), 3, dtype=np.int8)

    if day.height:
        days = np.interp(e, day["ehraid"].to_numpy(), day["t"].to_numpy())
        m = e >= day_start
        dd = [datetime.date(1970, 1, 1) + datetime.timedelta(days=int(round(x))) for x in days[m]]
        date[m] = [d.year * 10000 + d.month * 100 + d.day for d in dd]
        prec[m] = 1
    if yr.height:
        m = (e >= EHRAID_ORDERED) & (prec == 3)
        date[m] = np.floor(np.interp(e[m], yr["ehraid"].to_numpy(), yr["year"].to_numpy())).astype(np.int64)
        prec[m] = 2
    if exact.height:
        ex = out["date"]
        m = ex.is_not_null().to_numpy()
        date[m] = ex.filter(ex.is_not_null()).dt.strftime("%Y%m%d").cast(pl.Int64).to_numpy()
        prec[m] = 0

    payload = {
        "asOf": datetime.date.today().isoformat(),
        "uid": out["uid"].to_list(),
        "date": date.tolist(),
        "precision": prec.tolist(),
    }
    with gzip.open(OUT, "wt", compresslevel=9) as fh:
        json.dump(payload, fh, separators=(",", ":"))
    labels = {0: "exact (SHAB)", 1: "day from ehraid", 2: "year from ehraid", 3: "before ~1999"}
    for k, v in labels.items():
        n = int((prec == k).sum())
        print(f"  {v:18s} {n:7d} {n / len(prec):6.1%}")
    print(f"wrote {OUT} ({os.path.getsize(OUT) / 1e6:.1f} MB)")


main()
