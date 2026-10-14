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

Company data comes from Switzerland's official open sources: **LINDAS** (the commercial register as linked data), the **SHAB** (the official gazette, for new entries and deletions since 2018), the **cantonal commercial registers** (entry dates back to 1883) and the **swisstopo** address register (coordinates). The business registry is public information, updated regularly by the Swiss federal authorities.

The map tiles (the street map underneath) come from [OpenFreeMap](https://openfreemap.org/) using [OpenStreetMap](https://www.openstreetmap.org/) data — both fully open-source, with no API key and no rate limits.

**No data is collected from visitors.** There is no tracking, no cookies, no login.

---


## Technical notes

For those curious about how it works under the hood:

- Today's companies come from LINDAS (the commercial register as linked data, `scripts/active_firms.py`)
- Addresses are placed on their building with the swisstopo address register (`scripts/swisstopo_geo.py`)
- The coordinates are compressed and stored as gzipped JSON files, one per canton, fetched on demand — no server needed
- Parsing runs in a **web worker** so the UI stays responsive while large cantons load
- The map is created immediately and Bern's data streams in alongside the map style, so nothing waits on data before the first paint
- Canton and legal-form filters run on the GPU (deck.gl `DataFilterExtension`), so toggling them never re-uploads the points
- A separate **global search index** (`search_index.json.gz`, ~8 MB compressed) is downloaded, parsed, accent-folded and queried inside a second **web worker**, so searching 760k names never blocks the map
- A **service worker** caches data files after the first visit so the map loads fast on repeat visits
- CDN scripts are deferred, fonts load without blocking, and Bern's data is preloaded so the map is interactive as fast as possible
- Canton totals shown before a canton is loaded are generated from the data with `python3 scripts/canton_counts.py`
- The **timeline** (`data/timeline/`) has one lamp per company life: SHAB new entries and deletions since 2018, plus today's companies (LINDAS, placed by the swisstopo address register) lit from their cantonal register entry date back to 1883. `events.bin.gz` (3.9 MB) holds positions on a uint16 grid along a Morton curve, delta-coded and split into byte planes; names come in 65 regional chunks of ~180 kB, fetched on hover
- Caveats: before 2018 a lamp sits at the company's **current** address; companies deleted before 2018 are missing; the normal map and the timeline share one list of today's companies (LINDAS, `scripts/canton_files.py`), so at the end of the timeline every lit lamp sits on a map point
- The `'98` next to a company name is its entry year (`scripts/canton_files.py`): cantonal register, else SHAB, else estimate; known for 99.99% of companies
- The map renders entirely in your browser using [deck.gl](https://deck.gl) and [MapLibre GL](https://maplibre.org/)
- Keyboard navigation, ARIA labels, and color contrast meet WCAG AA accessibility standards
- The whole thing is a single HTML file + data files, hosted for free on GitHub Pages

### Data pipeline

```mermaid
flowchart TD
    A([LINDAS · Zefix linked data]) --> B[active_firms.py]
    S([swisstopo address register]) --> B
    B --> C[(build/lindas_active.parquet)]
    H([SHAB 2018–2026]) --> F[fetch_shab.py] --> E[(build/shab_events.jsonl)]
    R([cantonal registers]) --> K[fetch_cantonal.py] --> RG[data/registered.json.gz]
    C --> CF[canton_files.py]
    E --> CF
    RG --> CF
    CF --> D[data/*_0.json.gz\nsearch_index.json.gz]
    C --> T[build_timeline.py]
    E --> T
    RG --> T
    T --> TL[data/timeline/\nevents.bin.gz · names/]
    D --> J([Browser\ndeck.gl · MapLibre GL])
    TL --> J
```

---

Built by [ExtraPolo.com](https://extrapolo.com) · Data: [opendata.swiss](https://opendata.swiss) · Map: [OpenFreeMap](https://openfreemap.org/) / [OpenStreetMap](https://www.openstreetmap.org/)
