#!/usr/bin/env python3
"""Geocoder — Mapbox batch, 50 req/sec with gzip."""
import json, csv, os, urllib.request, urllib.parse, sys, gzip

TOKEN      = os.environ.get("MAPBOX_TOKEN", "")
INPUT_DIR  = "hr_companies"
OUT_DIR    = "hr_companies"
CACHE_FILE = "scripts/geocode_cache.json"
BATCH_SIZE = 10000
PROCS      = 20

def geocode(addr):
    query = urllib.parse.quote(addr)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json?access_token={TOKEN}&country=CH&limit=1&types=address,postcode"
    try:
        req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=15) as r:
            ct = r.headers.get("Content-Encoding", "")
            data = r.read()
            if "gzip" in ct:
                data = gzip.decompress(data)
            data = json.loads(data)
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

cache = json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}
total_calls = len(cache)
MAX_CALLS = 500000
print(f"[INIT] Cache: {total_calls} entries | remaining: {MAX_CALLS - total_calls:,}", flush=True)

done = {}
for f in os.listdir(INPUT_DIR):
    if not f.endswith("_latlong.csv"): continue
    canton = f[10:-4]
    with open(os.path.join(INPUT_DIR, f), encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    done[canton] = sum(1 for r in rows if str(r.get("lat","")).strip())
print(f"[INIT] Done cantons: {sorted(done)}", flush=True)

for fname in sorted(os.listdir(INPUT_DIR)):
    if not fname.startswith("companies_") or "_latlong" in fname or not fname.endswith(".csv"):
        continue
    canton = fname[10:-4]
    if canton in done:
        print(f"[SKIP] {canton}", flush=True)
        continue

    fpath   = os.path.join(INPUT_DIR, fname)
    outpath = os.path.join(OUT_DIR, f"companies_{canton}_latlong.csv")
    print(f"\n[CANTON] {canton}", flush=True)

    with open(fpath, encoding="utf-8") as f:
        reader  = csv.DictReader(f)
        fields  = list(reader.fieldnames)
        out_f   = fields + ["lat", "lng"]
        batch   = []
        i       = 0

        with open(outpath, "w", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=out_f)
            writer.writeheader()

            for row in reader:
                a = addr_key(row)
                if a in cache:
                    coords = cache[a]
                    row["lat"] = coords[0] if coords else ""
                    row["lng"] = coords[1] if coords else ""
                else:
                    row["lat"] = ""
                    row["lng"] = ""
                    if total_calls < MAX_CALLS:
                        result = geocode(a)
                        cache[a] = result
                        if result:
                            total_calls += 1
                            row["lat"] = result[0]
                            row["lng"] = result[1]

                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    writer.writerows(batch)
                    batch = []
                    if i % 2000 == 0:
                        g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
                        print(f"  [PROG] {i:,} | calls: {total_calls} | geocoded: {g}", flush=True)
                i += 1

            if batch:
                writer.writerows(batch)

    g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
    print(f"  [DONE] {g}/{i} ({100*g//i}%)", flush=True)
    json.dump(cache, open(CACHE_FILE, "w"), ensure_ascii=False)

print(f"\n[ALL DONE] {len(cache)} cache entries | {total_calls} API calls", flush=True)
