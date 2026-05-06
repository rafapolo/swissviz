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

## Features

- **Interactive map** — visualise company locations across Swiss cantons
- **Canton & legal form filters** — toggle cantons and legal entity types
- **Night view mode** — adjust intensity slider for dark-mode visualisation
- **DE/EN language switcher** — switch between German and English (legend only, panel controls stay in English)
- **Geo-location** — detects your location and shows canton/country in bottom bar
- **Smooth initial animation** — on load, smoothly flies from Bern to starting position

---

## Data & Pipeline

1. **Download** — company data fetched from opendata.swiss, one CSV per canton (26 total)
2. **Geocode** — `scripts/geocode.py` queries the Mapbox Geocoding API, building each query as `"{street}, {postal_code} {locality}, Switzerland"`; a local cache (`geocode_cache.json`) makes runs resumable; rate and mode are controlled via flags (`--sleep`, `--append`, `--canton`, …)
3. **Chunk & compress** — geocoded rows serialised to JSON arrays and gzip-compressed into numbered chunks; a `<CANTON>.json` metadata file records the chunk count (`{"_chunks": N}`)
4. **Render** — static files only, no backend; browser fetches chunks on demand, decompresses with [pako](https://github.com/nodeca/pako), and renders with [deck.gl](https://deck.gl) on a [MapLibre GL](https://maplibre.org/) basemap using [CartoDB Dark Matter](https://carto.com/basemaps/) tiles (OSM data, no token required)
