# SwissViz

Interactive map of Swiss companies built with Open Data

---

<p align="center">
  <img src="sample.jpg" width="800">
</p>
<p align="center">
  <img src="sample2.jpg" width="800">
</p>

---

## Data

Source: [Zefix — Swiss Central Business Name Index](https://www.zefix.admin.ch) via [opendata.swiss](https://opendata.swiss)

Fields used: company name, legal form (Rechtsform), street, postal code, locality — one CSV per canton.

## Pipeline

```
Download (opendata.swiss)
  → Geocode (Mapbox API, scripts/geocode.py)
  → Chunk & gzip (data/<CANTON>_<N>.json.gz)
  → Load in browser (pako decompress → deck.gl ScatterplotLayer)
```

1. **Download** — raw CSVs from the CH open data portal, one per canton (26 total)
2. **Geocode** — `scripts/geocode.py` queries the Mapbox Geocoding API, building each query as `"{street}, {postal_code} {locality}, Switzerland"`; a local cache (`geocode_cache.json`) makes runs resumable; rate and mode are controlled via flags (`--sleep`, `--append`, `--canton`, …)
3. **Chunk & compress** — geocoded rows serialised to JSON arrays and gzip-compressed into numbered chunks; a `<CANTON>.json` metadata file records the chunk count (`{"_chunks": N}`)
4. **Render** — static files only, no backend; browser fetches chunks on demand, decompresses with [pako](https://github.com/nodeca/pako), and renders with [deck.gl](https://deck.gl) on a [Mapbox GL](https://docs.mapbox.com/mapbox-gl-js/) dark basemap
