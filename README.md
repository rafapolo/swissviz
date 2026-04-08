# SwissViz

Interactive map of Swiss companies built with [Mapbox GL JS](https://docs.mapbox.com/mapbox-gl-js/) and [deck.gl](https://deck.gl/).

## Setup

### Mapbox Token

Set your Mapbox public token as a repository secret named `MAPBOX_TOKEN` at:
`https://github.com/rafapolo/swissviz/settings/secrets/new`

The token is injected at build time — no token is stored in source.

### Local Development

```bash
npm install
npm run dev
```

Then open `http://localhost:3000`.

## Data

Company data lives in `hr_companies/` as per-canton CSV files. Geocoding scripts are in `scripts/`.

## Deployment

Push to `main` — GitHub Actions automatically deploys to GitHub Pages.
