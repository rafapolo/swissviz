#!/usr/bin/env python3
"""Geocoder - batch writes for speed, 10 req/sec."""
import json, csv, os, time, urllib.request, urllib.parse, sys

TOKEN      = "pk.eyJ1IjoicmFmYXBvbG8iLCJhIjoiY21hcnl2bDY4MGRpdjJqc2M3OGZ4cXdnZCJ9.fmEqBBXCyPDPwIPhKjAkCA"
INPUT_DIR  = "hr_companies"
CACHE_FILE = "scripts/geocode_cache.json"
OUT_DIR    = "hr_companies"
SLEEP_SEC  = 0.1
BATCH_SIZE = 5000

def geocode(addr):
    query = urllib.parse.quote(addr)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json?access_token={TOKEN}&country=CH&limit=1&types=address,postcode"
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            data = json.loads(r.read())
            features = data.get("features", [])
            if features:
                lng, lat = features[0]["geometry"]["coordinates"]
                return [round(lat, 7), round(lng, 7)]
    except Exception as e:
        print(f"  [ERR] {e}", flush=True)
    return None

def addr_key(row):
    s = (row.get("street","") or "").strip()
    p = (row.get("plz","") or "").strip().split(".")[0]
    l = (row.get("locality","") or "").strip()
    return f"{s}, {p} {l}, Switzerland".strip(", ")

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, separators=(",",":"))

cache = {}
if os.path.exists(CACHE_FILE):
    cache = json.load(open(CACHE_FILE))
total_calls = len(cache)
MAX_CALLS = 500000
print(f"[INIT] Will stop at {MAX_CALLS:,} API calls ({MAX_CALLS - total_calls:,} remaining)", flush=True)
print(f"[INIT] Cache: {len(cache)} entries, {total_calls} successful calls", flush=True)

done = {}
for f in os.listdir(INPUT_DIR):
    if not f.endswith("_latlong.csv"): continue
    canton = f[10:-4]
    with open(os.path.join(INPUT_DIR, f), encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    geocoded = sum(1 for r in rows if str(r.get("lat","")).strip())
    if geocoded > 0:
        done[canton] = geocoded
print(f"[INIT] Done cantons: {sorted(done)}", flush=True)
for c, n in sorted(done.items()):
    print(f"  {c}: {n:,} geocoded", flush=True)

for fname in sorted(os.listdir(INPUT_DIR)):
    if not fname.startswith("companies_") or "_latlong" in fname or not fname.endswith(".csv"):
        continue
    canton = fname[10:-4]
    if canton in done:
        print(f"\n[SKIP] {canton} already done", flush=True)
        continue

    fpath = os.path.join(INPUT_DIR, fname)
    outpath = os.path.join(OUT_DIR, f"companies_{canton}_latlong.csv")
    print(f"\n[CANTON] {canton}", flush=True)

    with open(fpath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames)
        out_fields = fields + ["lat", "lng"]

        with open(outpath, "w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=out_fields)
            writer.writeheader()

            batch = []
            for i, row in enumerate(reader):
                a = addr_key(row)

                # Write cached first
                if a and a in cache:
                    coords = cache[a]
                    if coords:
                        row["lat"] = coords[0]
                        row["lng"] = coords[1]
                    else:
                        row["lat"] = ""
                        row["lng"] = ""
                else:
                    row["lat"] = ""
                    row["lng"] = ""

                batch.append(row)

                # Geocode if not cached
                if a and a not in cache:
                    if total_calls >= MAX_CALLS:
                        # Save cache and exit
                        save_cache(cache)
                        print(f"\n[LIMIT] Reached {MAX_CALLS} calls. Saving and exiting.", flush=True)
                        g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
                        print(f"  {g} companies geocoded in this canton", flush=True)
                        exit(0)

                    result = geocode(a)
                    cache[a] = result
                    if result:
                        total_calls += 1
                        batch[-1]["lat"] = result[0]
                        batch[-1]["lng"] = result[1]
                    time.sleep(SLEEP_SEC)

                # Write batch
                if len(batch) >= BATCH_SIZE:
                    writer.writerows(batch)
                    batch = []

                    # Save cache periodically
                    if (i+1) % 2000 == 0:
                        save_cache(cache)
                        g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
                        print(f"  [PROG] {i+1} rows | calls: {total_calls} | geocoded: {g}", flush=True)

            # Write remaining
            if batch:
                writer.writerows(batch)

        # Final count
        g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
        total = i + 1
        print(f"  [DONE] {g}/{total} geocoded ({100*g//total}%)", flush=True)

    save_cache(cache)
    print(f"  [CACHE] {len(cache)} total entries", flush=True)

print(f"\n[ALL DONE] Cache: {len(cache)} entries, {total_calls} API calls", flush=True)
