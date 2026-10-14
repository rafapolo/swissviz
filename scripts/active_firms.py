#!/usr/bin/env python3
"""Every active company with its address, legal form and coordinates, for the long timeline.

Read from LINDAS (Zefix as linked data) and placed on its building by the
swisstopo address register (scripts/swisstopo_geo.py, as the SHAB timeline).
See tasks/linha-do-tempo-longa.md.

Output (outside git): build/lindas_active.parquet
    uid ("CHE-123.456.789"), name, legalForm (eCH-0097 code, "0106"), street,
    houseNumber, zip, town, canton, seat, lat, lng, prec

Usage (from the repo root, where build/adr.parquet is): python3 scripts/active_firms.py
"""
import io, os, sys, time

import polars as pl
import requests

sys.path.insert(0, os.path.dirname(__file__))

UA = "cantography-research"
OUT = "build/lindas_active.parquet"
PAGE = 50_000
QUERY = """
PREFIX schema: <http://schema.org/>
PREFIX locn: <http://www.w3.org/ns/locn#>
SELECT ?c ?uid ?name ?type ?street ?number ?zip ?town ?canton ?seat WHERE { GRAPH <https://lindas.admin.ch/foj/zefix> {
 ?c a <https://schema.ld.admin.ch/ZefixOrganisation> ; schema:identifier ?i .
 ?i schema:name "CompanyUID" ; schema:value ?uid .
 OPTIONAL { ?c schema:legalName ?name }
 OPTIONAL { ?c schema:additionalType ?type }
 OPTIONAL { ?c schema:address ?a .
   OPTIONAL { ?a locn:thoroughfare ?street } OPTIONAL { ?a locn:locatorDesignator ?number }
   OPTIONAL { ?a schema:postalCode ?zip } OPTIONAL { ?a schema:addressLocality ?town }
   OPTIONAL { ?a schema:addressRegion ?canton } }
 OPTIONAL { ?c <https://schema.ld.admin.ch/municipality> ?m } }
 OPTIONAL { ?m schema:name ?seat } }
ORDER BY ?c LIMIT %d OFFSET %d"""
FIELDS = ["street", "houseNumber", "zip", "town", "canton", "seat"]


def parse_rows(csv_bytes: bytes) -> pl.DataFrame:
    """One page of the SPARQL CSV -> one row per company, empty strings for missing address parts."""
    df = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=0)
    d = pl.col("uid").str.replace_all(r"\D", "")
    return (df.select(
        pl.format("CHE-{}.{}.{}", d.str.slice(0, 3), d.str.slice(3, 3), d.str.slice(6, 3)).alias("uid"),
        pl.col("name").fill_null(""),
        pl.col("type").str.extract(r"(\d{4})$").alias("legalForm"),
        *[pl.col(src).fill_null("").alias(dst) for src, dst in
          zip(["street", "number", "zip", "town", "canton", "seat"], FIELDS)],
    ).unique("uid", keep="first", maintain_order=True))


def fetch_all() -> pl.DataFrame:
    parts, off = [], 0
    while True:
        for attempt in range(5):
            try:
                r = requests.post("https://ld.admin.ch/query", data={"query": QUERY % (PAGE, off)}, timeout=600,
                                  headers={"Accept": "text/csv", "User-Agent": UA})
                r.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"  retry {attempt + 1}: {e}", flush=True)
                time.sleep(10 * (attempt + 1))
        else:
            raise RuntimeError("LINDAS failed")
        page = parse_rows(r.content)
        print(f"  LINDAS offset {off}: {page.height}", flush=True)
        parts.append(page)
        # a company with several addresses spans rows; a full page still means more to read
        if len(r.content.splitlines()) - 1 < PAGE:
            break
        off += PAGE
    return pl.concat(parts).unique("uid", keep="first", maintain_order=True)


def main():
    from swisstopo_geo import geocode
    df = fetch_all()
    print(f"[LINDAS] {df.height:,} active companies, legal form missing in {df['legalForm'].null_count():,}")
    df = geocode(df)
    df.write_parquet(OUT)
    print("[GEO] precision:")
    for prec, n in df.group_by("prec").len().sort("len", descending=True).iter_rows():
        print(f"   {prec:9} {n:8,}  {100 * n / df.height:5.1f}%")
    print(f"[OUT] {OUT}")


if __name__ == "__main__":
    main()
