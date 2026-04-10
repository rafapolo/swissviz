#!/usr/bin/env python3
"""Mapbox geocoder — unified script.

Usage:
    python scripts/geocode.py [options]

Options:
    --sleep SECS      Seconds between API calls (default: 0 = fastest)
    --batch INT       Rows per write flush (default: 10000)
    --max-calls INT   Max API calls before stopping (default: 500000)
    --canton CODE     Process only this canton, e.g. --canton ZH
    --append          Append to existing output instead of skipping done cantons
    --no-gzip         Disable gzip compression on requests

Token:
    Set MAPBOX_TOKEN env var before running.
    e.g.  MAPBOX_TOKEN=pk.xxx python scripts/geocode.py --sleep 0.05
"""
import argparse, csv, gzip, json, os, time, urllib.request, urllib.parse

# ── Args ──────────────────────────────────────────────────────────────────────
p = argparse.ArgumentParser(description="Geocode Swiss company CSVs via Mapbox")
p.add_argument("--sleep",     type=float, default=0,       metavar="SECS",  help="sleep between calls (0 = no limit)")
p.add_argument("--batch",     type=int,   default=10000,   metavar="INT",   help="rows per write flush")
p.add_argument("--max-calls", type=int,   default=500_000, metavar="INT",   help="stop after N API calls")
p.add_argument("--canton",    type=str,   default=None,    metavar="CODE",  help="process only this canton")
p.add_argument("--append",    action="store_true",                          help="append to existing output (resume mid-canton)")
p.add_argument("--no-gzip",   action="store_true",                          help="disable gzip request headers")
args = p.parse_args()

TOKEN = os.environ.get("MAPBOX_TOKEN", "")
if not TOKEN:
    raise SystemExit("Error: MAPBOX_TOKEN environment variable not set.")

INPUT_DIR  = "hr_companies"
OUT_DIR    = "hr_companies"
CACHE_FILE = "scripts/geocode_cache.json"
USE_GZIP   = not args.no_gzip

# ── Geocode ───────────────────────────────────────────────────────────────────
def geocode(addr):
    query = urllib.parse.quote(addr)
    url   = (f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json"
             f"?access_token={TOKEN}&country=CH&limit=1&types=address,postcode")
    try:
        req = urllib.request.Request(url)
        if USE_GZIP:
            req.add_header("Accept-Encoding", "gzip")
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            if USE_GZIP and "gzip" in r.headers.get("Content-Encoding", ""):
                raw = gzip.decompress(raw)
            features = json.loads(raw).get("features", [])
            if features:
                lng, lat = features[0]["geometry"]["coordinates"]
                return [round(lat, 7), round(lng, 7)]
    except Exception as e:
        print(f"  [ERR] {e}", flush=True)
    return None

def addr_key(row):
    s = (row.get("street",   "") or "").strip()
    p = (row.get("plz",      "") or "").strip().split(".")[0]
    l = (row.get("locality", "") or "").strip()
    return f"{s}, {p} {l}, Switzerland".strip(", ")

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, separators=(",", ":"))

# ── Load cache ────────────────────────────────────────────────────────────────
cache       = json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}
total_calls = len(cache)
print(f"[INIT] Cache: {total_calls:,} entries | remaining quota: {args.max_calls - total_calls:,}", flush=True)
print(f"[INIT] sleep={args.sleep}s | batch={args.batch} | gzip={USE_GZIP} | append={args.append}", flush=True)

# ── Discover done cantons ─────────────────────────────────────────────────────
done = {}
for f in os.listdir(INPUT_DIR):
    if not f.endswith("_latlong.csv"):
        continue
    canton = f[len("companies_"):-len("_latlong.csv")]
    with open(os.path.join(INPUT_DIR, f), encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    done[canton] = sum(1 for r in rows if str(r.get("lat", "")).strip())
print(f"[INIT] Done cantons: {sorted(done)}", flush=True)

# ── Process ───────────────────────────────────────────────────────────────────
for fname in sorted(os.listdir(INPUT_DIR)):
    if not fname.startswith("companies_") or "_latlong" in fname or not fname.endswith(".csv"):
        continue

    canton = fname[len("companies_"):-len(".csv")]
    if args.canton and canton != args.canton.upper():
        continue

    if not args.append and canton in done:
        print(f"[SKIP] {canton} ({done[canton]:,} already geocoded)", flush=True)
        continue

    fpath   = os.path.join(INPUT_DIR, fname)
    outpath = os.path.join(OUT_DIR, f"companies_{canton}_latlong.csv")
    mode    = "a" if args.append else "w"
    print(f"\n[CANTON] {canton}", flush=True)

    with open(fpath, encoding="utf-8") as f:
        reader    = csv.DictReader(f)
        out_fields = list(reader.fieldnames) + ["lat", "lng"]
        batch     = []
        i         = 0

        with open(outpath, mode, newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=out_fields, lineterminator="\n")
            if mode == "w" or os.stat(outpath).st_size == 0:
                writer.writeheader()

            for row in reader:
                a = addr_key(row)

                if a in cache:
                    coords     = cache[a]
                    row["lat"] = coords[0] if coords else ""
                    row["lng"] = coords[1] if coords else ""
                else:
                    row["lat"] = ""
                    row["lng"] = ""
                    if total_calls < args.max_calls:
                        if total_calls >= args.max_calls:
                            save_cache(cache)
                            print(f"\n[LIMIT] Reached {args.max_calls:,} calls. Stopping.", flush=True)
                            raise SystemExit(0)
                        result     = geocode(a)
                        cache[a]   = result
                        if result:
                            total_calls   += 1
                            row["lat"]     = result[0]
                            row["lng"]     = result[1]
                        if args.sleep:
                            time.sleep(args.sleep)

                batch.append(row)
                if len(batch) >= args.batch:
                    writer.writerows(batch)
                    batch = []
                    save_cache(cache)
                    g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat", ""))
                    print(f"  [PROG] {i:,} rows | calls: {total_calls:,} | geocoded: {g:,}", flush=True)
                i += 1

            if batch:
                writer.writerows(batch)

    g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat", ""))
    print(f"  [DONE] {g:,}/{i:,} ({100 * g // max(i, 1)}%)", flush=True)
    save_cache(cache)

print(f"\n[ALL DONE] Cache: {len(cache):,} entries | {total_calls:,} API calls", flush=True)
