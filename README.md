# Pathfinder

Risk-aware, offline-friendly maps and routing tools for Sudan–Chad
humanitarian operations. Pathfinder fuses fresh satellite imagery,
OpenStreetMap edits, and conflict-event data to publish daily safe-route
layers (GeoJSON, MBTiles, printable PDFs) for refugees, local drivers,


---
## Data license

* **OpenStreetMap layers** © OpenStreetMap contributors, released under the
  Open Database License (ODbL) v1.0.
* **Conflict-event CSV (ACLED)** © ACLED. Free for non-commercial, attribution
  required.
* **Satellite imagery** © Planet Labs PBC (NICFI program). Redistribution of
  raw imagery is prohibited; derived vector layers released as CC-BY-SA 4.0.



## Quick start (tech)

```bash
# clone & spin up complete stack
git clone https://github.com/JVCarmichael0959/pathfinder.git
cd pathfinder
docker compose up      # PostGIS + pgRouting + Jupyter + Flask

## Data ingest (raw ➜ PostGIS)

| script | purpose | example call |
|--------|---------|--------------|
| `scripts/pull_acled.py` | Pull ACLED events for one or more countries/regions (14-day window by default) | `python scripts/pull_acled.py Sudan Chad` |
| `scripts/fetch_hdx_sa_monthly.py` | South-Africa monthly aggregates (events & fatalities) → CSV + PostGIS | `python scripts/fetch_hdx_sa_monthly.py "<HDX-xlsx-URL>"` |
| `scripts/fetch_hdx_sudan_roads.py` | HOT-OSM Sudan roads export (ZIP) → GPKG + PostGIS | `python scripts/fetch_hdx_sudan_roads.py "<roads-zip-URL>"` |

All three write raw files to **`data/raw/`** and populate the corresponding PostGIS tables inside the `db` container (`events_raw`, `sa_monthly_violence`, `sudan_roads_osm`).


