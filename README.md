# Pathfinder

Risk-aware, offline-friendly mapping tools for humanitarian operations in the Sudan–Chad region. Pathfinder fuses fresh satellite imagery, recent OpenStreetMap edits and ACLED conflict data to produce daily safe-route layers (GeoJSON, MBTiles and printable PDFs).

## Project goals

* Provide up‑to‑date road safety information for refugees and local drivers.
* Offer a simple repeatable data pipeline (ETL) that runs in Docker or CI.
* Publish lightweight map packages and a small dashboard for rapid situational awareness.

## Event Visualization

Animated visualization showing geolocated ACLED events color-coded by event type:

![Sudan Events Animation](assets/sudan_events_colored.gif)

## Repository layout

```
.
├── dashboard/     # Streamlit app for exploring monthly event metrics
├── data/          # raw downloads saved by the ETL scripts
├── maps/          # HTML map prototypes and generated tiles
├── notebooks/     # Jupyter notebooks for analysis and prototyping
├── pathfinder/    # tiny Python package with DB utilities and queries
├── scripts/       # command line tools for pulling + loading data
├── scratch/       # experimental loaders / one‑off helpers
├── sql/           # PostGIS SQL for cleaning and summarising tables
└── docker-compose.yml  # local PostGIS + Jupyter stack
```

Raw files live under **`data/raw/`** and the ETL scripts populate the PostGIS database running in the `db` container. The main tables are `events_raw`, `sa_monthly_violence` and `sudan_roads_osm`.

## Quick start

```bash
# clone & start the stack
git clone <repo-url>
cd Pathfinder
pip install -e .    # install the package for local tests
cp .env.example .env   # fill in your ACLED credentials
docker compose up      # brings up PostGIS and Jupyter

# alternatively, use the helper script
bash scripts/setup_dev_env.sh

# first-run helpers
python scripts/bootstrap_roads.py
python scratch/load_pv_monthly.py
streamlit run dashboard/app.py
```

### Running tests

Install the package locally and run pytest:

```bash
pip install -e .
pytest
```

### Data ingest

| script | purpose | example call |
|--------|---------|--------------|
| `scripts/pull_acled.py` | Pull ACLED events for one or more countries/regions (14‑day window by default) | `python scripts/pull_acled.py Sudan Chad` |
| `scripts/fetch_hdx_sa_monthly.py` | South-Africa monthly aggregates (events & fatalities) → CSV + PostGIS | `python scripts/fetch_hdx_sa_monthly.py "<HDX-xlsx-URL>"` |
| `scripts/fetch_hdx_sudan_roads.py` | HOT‑OSM Sudan roads export (ZIP) → GPKG + PostGIS | `python scripts/fetch_hdx_sudan_roads.py "<roads-zip-URL>"` |
| `scripts/bootstrap_roads.py` | Create base road tables if missing | `python scripts/bootstrap_roads.py` |
| `scripts/fetch_hdx_pv.sh` | Download Sudan political‑violence data from HDX | `bash scripts/fetch_hdx_pv.sh` |
| `scratch/load_pv_monthly.py` | Ingest monthly Sudan violence workbook | `python scratch/load_pv_monthly.py` |
| `scripts/enrich_admin2.py` | Load admin boundaries and enrich monthly events | `python scripts/enrich_admin2.py` |
| `scripts/plot_monthly_totals.py` | Plot events & fatalities from PostGIS into `output.png` | `python scripts/plot_monthly_totals.py` |
| `sql/03_geo_join.sql` | Materialised view of events within 5 km of primary roads | `psql -f sql/03_geo_join.sql` |
| `sql/04_last12months_view.sql` | Materialised view of the last 12 months metrics | `psql -f sql/04_last12months_view.sql` |

## Dashboard

Run the Streamlit dashboard showing the last twelve months of ACLED data:

```bash
streamlit run dashboard/app.py
```

The sidebar lets you pick how many months to display and filter by **Admin1** region. Charts update automatically.

## Risk modelling & routing

Run `scripts/update_risk_layers.py` to rebuild Bayesian road risk scores. Use
`scripts/export_routes.py` to generate a suggested route based on these scores
(OR‑Tools is used if available). `scripts/validate_risk.py` prints a simple RMSE
metric comparing predictions to the most recent month of events.

## Data license

* **OpenStreetMap layers** © OpenStreetMap contributors, released under the Open Database License (ODbL) v1.0.
* **Conflict‑event CSV (ACLED)** © ACLED. Free for non‑commercial use; attribution required.
* **Satellite imagery** © Planet Labs PBC (NICFI program). Redistribution of raw imagery is prohibited; derived vector layers released as CC‑BY‑SA 4.0.
