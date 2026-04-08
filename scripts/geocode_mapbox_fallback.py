#!/usr/bin/env python3
"""Fallback batch geocoder — slower rate, resumes from cache."""
import json, csv, os, urllib.request, urllib.parse

TOKEN      = os.environ.get("MAPBOX_TOKEN", "")
INPUT_DIR  = "hr_companies"
OUT_DIR    = "hr_companies"
CACHE_FILE = "scripts/geocode_cache.json"
SLEEP_SEC  = 0.05
MAX_CALLS  = 500000

def geocode(addr):
    query = urllib.parse.quote(addr)
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{query}.json?access_token={TOKEN}&country=CH&limit=1&types=address,postcode"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
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

cache = json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}
total_calls = len(cache)
print(f"[INIT] Cache: {total_calls} entries | remaining: {MAX_CALLS - total_calls:,}", flush=True)

for fname in sorted(os.listdir(INPUT_DIR)):
    if not fname.startswith("companies_") or "_latlong" in fname or not fname.endswith(".csv"):
        continue
    canton = fname[10:-4]
    fpath   = os.path.join(INPUT_DIR, fname)
    outpath = os.path.join(OUT_DIR, f"companies_{canton}_latlong.csv")

    if os.path.exists(outpath):
        with open(outpath, encoding="utf-8") as fh:
            existing = set(row["street"] + row["plz"] + row["locality"] for row in csv.DictReader(fh) if row.get("lat",""))
        print(f"[SKIP] {canton} ({len(existing)} done)", flush=True)

    print(f"\n[CANTON] {canton}", flush=True)
    batch = []
    i = 0

    with open(fpath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames)
        out_f  = fields + ["lat", "lng"]

        with open(outpath, "a", newline="", encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=out_f, lineterminator="\n")
            if os.stat(outpath).st_size == 0:
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
                        import time; time.sleep(SLEEP_SEC)

                batch.append(row)
                if len(batch) >= 5000:
                    writer.writerows(batch)
                    batch = []
                    if i % 1000 == 0:
                        print(f"  [PROG] {i:,} | calls: {total_calls}", flush=True)
                i += 1

            if batch:
                writer.writerows(batch)

    g = sum(1 for r in csv.DictReader(open(outpath, encoding="utf-8")) if r.get("lat",""))
    print(f"  [DONE] {g}/{i} ({100*g//i}%)", flush=True)
    json.dump(cache, open(CACHE_FILE, "w"), ensure_ascii=False)

print(f"\n[ALL DONE] {len(cache)} cache entries | {total_calls} API calls", flush=True)
