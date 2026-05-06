#!/usr/bin/env python3
"""Repack data/*.json.gz into smaller columnar format.

Transformations per file:
  - Drop 'm' (municipality) field — unused in the app
  - Reduce lat/lng to 4 decimal places
  - Encode 't' (legal form) as single char

Usage:
  python3 scripts/repack.py --out ./data_v2   # dry run into new dir
  python3 scripts/repack.py --out ./data       # overwrite in place
"""

import argparse
import gzip
import json
import os
import glob

T_MAP = {
    "Aktiengesellschaft":               "A",
    "Ausländische Niederlassung":       "L",
    "Besondere Rechtsform":             "B",
    "Einzelunternehmen":                "E",
    "Genossenschaft":                   "N",
    "GmbH / SARL":                      "G",
    "Haupt von Gemeinderschaften":      "H",
    "Institut des öffentlichen Rechts": "I",
    "Kollektivgesellschaft":            "K",
    "Kommanditgesellschaft":            "O",
    "Schweizerische Zweigniederlassung":"Z",
    "Stiftung":                         "S",
    "Verein":                           "V",
}


def repack_file(src_path, dst_path):
    with gzip.open(src_path, "rt", encoding="utf-8") as f:
        records = json.load(f)

    col = {"n": [], "t": [], "lat": [], "lng": []}
    unmapped = set()

    for r in records:
        t_raw = r.get("t", "")
        t_code = T_MAP.get(t_raw)
        if t_code is None:
            unmapped.add(t_raw)
            t_code = t_raw  # fallback: keep original

        col["n"].append(r.get("n", ""))
        col["t"].append(t_code)
        col["lat"].append(round(float(r.get("lat", 0)), 4))
        col["lng"].append(round(float(r.get("lng", 0)), 4))

    packed = json.dumps(col, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    compressed = gzip.compress(packed, compresslevel=9)

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    with open(dst_path, "wb") as f:
        f.write(compressed)

    src_size = os.path.getsize(src_path)
    dst_size = len(compressed)
    return src_size, dst_size, len(records), unmapped


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="Output directory for repacked files")
    parser.add_argument("--data", default="data", help="Input data directory (default: data)")
    args = parser.parse_args()

    src_dir = args.data.rstrip("/")
    dst_dir = args.out.rstrip("/")

    files = sorted(glob.glob(f"{src_dir}/*.json.gz"))
    if not files:
        print(f"No *.json.gz files found in {src_dir}/")
        return

    total_src = total_dst = total_records = 0
    all_unmapped = set()

    print(f"{'File':<20} {'Before':>8} {'After':>8} {'Savings':>8} {'Records':>8}")
    print("-" * 60)

    for src_path in files:
        fname = os.path.basename(src_path)
        dst_path = os.path.join(dst_dir, fname)
        src_size, dst_size, n_records, unmapped = repack_file(src_path, dst_path)
        savings_pct = (1 - dst_size / src_size) * 100 if src_size else 0
        total_src += src_size
        total_dst += dst_size
        total_records += n_records
        all_unmapped |= unmapped
        print(f"{fname:<20} {src_size/1024:>7.0f}K {dst_size/1024:>7.0f}K {savings_pct:>7.1f}% {n_records:>8,}")

    print("-" * 60)
    total_savings = (1 - total_dst / total_src) * 100 if total_src else 0
    print(f"{'TOTAL':<20} {total_src/1024/1024:>7.1f}M {total_dst/1024/1024:>7.1f}M {total_savings:>7.1f}% {total_records:>8,}")
    print(f"\nOutput written to: {dst_dir}/")

    if all_unmapped:
        print(f"\nWARNING — unmapped legal forms (kept as-is): {all_unmapped}")
    else:
        print("\nAll legal forms mapped successfully.")


if __name__ == "__main__":
    main()
