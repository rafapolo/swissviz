# SwissViz

An interactive map of every registered company in Switzerland — all 750,000+ of them.

---

<p align="center">
  <img src="sample2.jpg" width="800">
</p>

---

## What is this?

SwissViz turns Switzerland's official public business registry into a visual map you can explore. Each dot on the map is a real, registered company. The colour tells you what kind of company it is — corporations in yellow, foundations in purple, sole traders in orange, and so on.

**Live at → [rafapolo.github.io/swissviz](https://rafapolo.github.io/swissviz)**

No account needed. No install. Just open the link.

---

## How to use it

### Navigating the map
- **Scroll** (or pinch on mobile) to zoom in and out
- **Click and drag** to move around
- **Hover any dot** to see the companies at that address; **click** to pin the card so you can select text and open each company in the official register ([Zefix](https://www.zefix.ch)) with the ↗ link. Click empty map or press **Esc** to close it
- **Share what you see**: the address bar keeps the zoom, position, cantons, legal-form filters and night view, so copying the link shares the exact view

### Searching for a company
Type any company name in the **search bar** at the top of the map (or press **/** from anywhere). Results search across all 760,000+ companies — even ones in cantons you haven't loaded yet — ignore accents (*zurich* finds *Zürich*) and list names that start with your text first.

- Use **↑ / ↓ arrow keys** to move through results, **Enter** to select, **Esc** to close
- Click any result to fly directly to that company on the map — a transparent green ring marker appears on the exact dot for 3 seconds, and its card opens with that company on top
- If a company name matches multiple locations (e.g. a chain with branches), all locations are marked simultaneously and the map fits to show them all
- If the company's canton wasn't loaded yet, it loads automatically

### Left panel — Cantons
Choose which cantons (regions) to show on the map. By default Bern is loaded. Click any canton name to load its companies (the map flies there if it's off-screen). You can select all or none with the buttons at the top; with *Select all* each canton appears as soon as it arrives. If a canton fails to load, a message offers to retry.

### Right panel — Legend
The legend shows the 12 legal forms and their colours. Click any item to hide or show that company type. The number in brackets shows how many companies of that type are visible.

Switch the whole interface between **DE**, **FR**, **IT** and **EN** using the buttons in the top-right of the legend. The first visit follows your browser's language, and your choice is remembered.

### Night view
At the bottom of the legend panel there's a **Night view** toggle. It turns all dots the same warm amber colour, making the density of business activity stand out against the dark map — great for seeing which areas are most commercially active.

### Collapsing the panels
On desktop, click the **‹** or **›** arrow inside each panel header to slide it out of the way. Click the icon button that appears in the corner to bring it back.

On mobile, tap the corner icons to open panels as bottom sheets. Tap anywhere on the map to close them.

---

## Where does the data come from?

All data is sourced from **[opendata.swiss](https://opendata.swiss)** — Switzerland's official open government data portal. The business registry is public information, updated regularly by the Swiss federal authorities.

The map tiles (the street map underneath) come from [OpenFreeMap](https://openfreemap.org/) using [OpenStreetMap](https://www.openstreetmap.org/) data — both fully open-source, with no API key and no rate limits.

**No data is collected from visitors.** There is no tracking, no cookies, no login.

---


## Technical notes

For those curious about how it works under the hood:

- Company data is downloaded from opendata.swiss (one CSV file per canton, 26 total)
- Addresses are geocoded (converted to map coordinates) using the Mapbox Geocoding API
- The coordinates are compressed and stored as gzipped JSON files, one per canton, fetched on demand — no server needed
- Parsing runs in a **web worker** so the UI stays responsive while large cantons load
- The map is created immediately and Bern's data streams in alongside the map style, so nothing waits on data before the first paint
- Canton and legal-form filters run on the GPU (deck.gl `DataFilterExtension`), so toggling them never re-uploads the points
- A separate **global search index** (`search_index.json.gz`, ~8 MB compressed) is downloaded, parsed, accent-folded and queried inside a second **web worker**, so searching 760k names never blocks the map
- A **service worker** caches data files after the first visit so the map loads fast on repeat visits
- CDN scripts are deferred, fonts load without blocking, and Bern's data is preloaded so the map is interactive as fast as possible
- Canton totals shown before a canton is loaded are generated from the data with `python3 scripts/canton_counts.py`
- The map renders entirely in your browser using [deck.gl](https://deck.gl) and [MapLibre GL](https://maplibre.org/)
- Keyboard navigation, ARIA labels, and color contrast meet WCAG AA accessibility standards
- The whole thing is a single HTML file + data files, hosted for free on GitHub Pages

### Data pipeline

```mermaid
flowchart TD
    A([opendata.swiss]) -->|CSV per canton\n26 files| B[hr_companies/\n*.csv]

    B --> C[geocode.py]
    E[(geocode_cache.json\nresume support)] <-->|cache lookup\n& save| C
    F([Mapbox Geocoding API]) -->|lat · lng| C
    C -->|address → coordinates| G[hr_companies/\n*_latlong.csv]

    G --> H[repack.py]
    H -->|columnar JSON\nsingle-char type codes\n4-decimal coords\ngzip compressed| I[data/\n*.json.gz chunks]

    I -->|fetched on demand\ndecompressed in browser| J([Browser\ndeck.gl · MapLibre GL\nOpenFreeMap tiles])
```

---

Built by [ExtraPolo.com](https://extrapolo.com) · Data: [opendata.swiss](https://opendata.swiss) · Map: [OpenFreeMap](https://openfreemap.org/) / [OpenStreetMap](https://www.openstreetmap.org/)
