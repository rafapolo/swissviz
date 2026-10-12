#!/usr/bin/env python3
"""Download every SHAB new entry (HR01) and deletion (HR03) since 2018.

Step 1 pages the publication list month by month and keeps the ids in
build/shab_ids.parquet. Step 2 fetches each publication's XML and appends the
parsed company fields to build/shab_events.jsonl, one line per id, so a re-run
skips what is already there.

Usage:
    python3 scripts/fetch_shab.py                # list + details
    python3 scripts/fetch_shab.py --list-only
    python3 scripts/fetch_shab.py --threads 8 --start 2018-01 --end 2026-10
"""
import argparse, datetime as dt, json, os, random, threading, time
import urllib.error, urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl

API = "https://amtsblattportal.ch/api/v1/publications"
IDS = "build/shab_ids.parquet"
EVENTS = "build/shab_events.jsonl"
KIND = {"HR01": "open", "HR03": "close"}

p = argparse.ArgumentParser()
p.add_argument("--start", default="2018-01")
p.add_argument("--end", default=dt.date.today().strftime("%Y-%m"))
p.add_argument("--threads", type=int, default=8)
p.add_argument("--list-only", action="store_true")
p.add_argument("--refresh-list", action="store_true", help="re-page the list even if the parquet exists")
args = p.parse_args()


def get(url, accept="application/json", tries=6):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"Accept": accept, "User-Agent": "cantography/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            err = e
        except Exception as e:
            err = e
        time.sleep(min(60, 2 ** i) + random.random())
    raise RuntimeError(f"{url}: {err}")


def months(start, end):
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    while (y, m) <= (ey, em):
        first = dt.date(y, m, 1)
        nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
        yield first, nxt - dt.timedelta(days=1)
        y, m = nxt.year, nxt.month


def list_window(first, last):
    """One page of up to 3000 ids; a window that holds more is split in two.
    Paging a large window is unreliable: the list has no fixed order, so pages
    can overlap, and the API refuses offsets near 10000."""
    url = (f"{API}?subRubrics=HR01,HR03&publicationStates=PUBLISHED"
           f"&publicationDate.start={first}&publicationDate.end={last}"
           f"&pageRequest.size=3000&pageRequest.page=0")
    d = json.loads(get(url))
    if d["total"] > 3000:
        if first == last:
            raise RuntimeError(f"{first}: {d['total']} publications in one day")
        mid = first + (last - first) // 2
        return list_window(first, mid) + list_window(mid + dt.timedelta(days=1), last)
    rows = []
    for c in d["content"]:
        m = c["meta"]
        rows.append({
            "id": m["id"],
            "sub": m["subRubric"],
            "pub": m["publicationDate"][:10],
            "title": (m.get("title") or {}).get("de", ""),
            "cantons": ",".join(m.get("cantons") or []),
        })
    if len(rows) != d["total"]:
        print(f"  [WARN] {first}..{last}: got {len(rows)} of {d['total']}", flush=True)
    return rows


def build_list():
    rows = []
    for first, last in months(args.start, args.end):
        r = list_window(first, last)
        n1 = sum(x["sub"] == "HR01" for x in r)
        print(f"[LIST] {first:%Y-%m}  HR01 {n1:6,}  HR03 {len(r) - n1:6,}", flush=True)
        rows += r
    df = pl.DataFrame(rows)
    dups = df.height - df["id"].n_unique()
    if dups:
        print(f"  [WARN] {dups:,} duplicate ids across windows", flush=True)
    df = df.unique("id", keep="first")
    os.makedirs("build", exist_ok=True)
    df.write_parquet(IDS)
    print(f"[LIST] {df.height:,} ids -> {IDS}", flush=True)
    return df


def text(el, path):
    x = el.find(path) if el is not None else None
    return x.text.strip() if x is not None and x.text else ""


def parse(xml, row):
    root = ET.fromstring(xml)
    content = root.find("content")
    company = content.find("commonsNew/company")
    if company is None:
        company = content.find("commonsActual/company")
    addr = company.find("address") if company is not None else None
    return {
        "id": row["id"],
        "kind": KIND[row["sub"]],
        "date": text(content, "journalDate") or row["pub"],
        "pub": row["pub"],
        "uid": text(company, "uid"),
        "name": text(company, "name"),
        "legalForm": text(company, "legalForm"),
        "street": text(addr, "street"),
        "houseNumber": text(addr, "houseNumber"),
        "zip": text(addr, "swissZipCode"),
        "town": text(addr, "town"),
        "seat": text(company, "seat"),
        "cantons": row["cantons"],
    }


def fetch_details(df):
    done = set()
    if os.path.exists(EVENTS):
        with open(EVENTS, encoding="utf-8") as f:
            for line in f:
                try:
                    done.add(json.loads(line)["id"])
                except Exception:
                    pass
    todo = [r for r in df.iter_rows(named=True) if r["id"] not in done]
    print(f"[XML] {len(done):,} cached, {len(todo):,} to fetch", flush=True)

    lock = threading.Lock()
    out = open(EVENTS, "a", encoding="utf-8")
    if out.tell():
        with open(EVENTS, "rb") as f:
            f.seek(-1, os.SEEK_END)
            if f.read(1) != b"\n":
                out.write("\n")
    t0, n, failed = time.time(), 0, 0

    def work(row):
        xml = get(f"{API}/{row['id']}/xml", accept="application/xml")
        return parse(xml, row) if xml else None

    with ThreadPoolExecutor(args.threads) as ex:
        futs = {ex.submit(work, r): r for r in todo}
        for fut in as_completed(futs):
            try:
                rec = fut.result()
            except Exception as e:
                failed += 1
                print(f"  [ERR] {futs[fut]['id']}: {e}", flush=True)
                continue
            if rec is None:
                failed += 1
                continue
            with lock:
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1
                if n % 5000 == 0:
                    out.flush()
                    rate = n / (time.time() - t0)
                    eta = (len(todo) - n) / max(rate, 1e-9) / 60
                    print(f"  [XML] {n:,}/{len(todo):,}  {rate:.0f}/s  eta {eta:.0f} min", flush=True)
    out.close()
    print(f"[XML] done: {n:,} new, {failed:,} failed", flush=True)


df = pl.read_parquet(IDS) if os.path.exists(IDS) and not args.refresh_list else build_list()
if not args.list_only:
    fetch_details(df)
