# Pathfinder

## Overview

Pathfinder provides risk-aware, offline-friendly mapping tools for humanitarian work in the Sudan–Chad region by blending satellite imagery, recent OpenStreetMap road data, and ACLED conflict feeds to produce daily safe-route layers along with printable products. The quick-start guide below covers installing the Python package, configuring ACLED credentials, bringing up the Docker stack, and launching first-run helpers that load baseline datasets and the Streamlit dashboard.

## Project goals

* Provide up‑to‑date road safety information for refugees and local drivers.
* Offer a simple repeatable data pipeline (ETL) that runs in Docker or CI.
* Publish lightweight map packages and a small dashboard for rapid situational awareness.

## Key components

### Python package (`pathfinder/`)

* `db.get_engine()` centralizes SQLAlchemy connection management, prioritizing a `DATABASE_URL` override while defaulting to Postgres credentials used by the Docker stack.
* `bayesian.py` implements a Gamma-Poisson model for estimating event rates per admin2, joining those predictions back onto primary roads and writing refreshed risk scores into PostGIS tables.
* `risk_tsp.py` assembles risk-weighted distance matrices, offers both a greedy nearest-neighbor heuristic and an optional OR-Tools solver, and returns ordered road itineraries to support safer routing decisions.
* `queries.py` contains higher-level Pandas/SQL helpers such as monthly totals aggregation and road type counts that are reused by plotting scripts and notebooks.
* Supporting utilities include cached engine access (`settings.py`) and standardized logging configuration used across ETL scripts.

### ETL & automation

* `pathfinder/etl/pull_acled.py` authenticates against the ACLED API, caches ISO codes, builds queries for both individual countries and regional aliases, saves raw CSV snapshots, and writes refreshed event data into PostGIS, exiting with helpful error handling when input validation or network calls fail.
* `pathfinder/etl/enrich_admin2.py` ensures PostGIS extensions are enabled, loads shapefiles via `ogr2ogr`, and joins geographic admin boundaries onto monthly ACLED aggregates, rebuilding indexes for spatial queries.
* Thin wrapper scripts in `scripts/` orchestrate workflows: e.g., `update_risk_layers.py` recalculates road risk tables, `export_routes.py` renders planned routes in Folium (optionally exporting to PDF if WeasyPrint is installed), and CLI entry points expose ETL modules directly.
* The ingest table below summarises additional helpers for HDX data pulls, bootstrap loading, plotting, and SQL materialized views, showing how the project assembles its data lake and analytics outputs.

### Dashboard & visualization

* `dashboard/app.py` is a Streamlit application that caches PostGIS queries for the last 12 months, provides sidebar filters for month count and Admin1 region, displays key metrics, Altair line charts and heatmaps, and lets analysts download filtered CSVs for follow-up work.
* Command-line script `plot_monthly_totals.py` offers a quick matplotlib PNG export of events and fatalities using the shared `monthly_totals` query.

### Infrastructure & dependencies

* `docker-compose.yml` provisions a PostGIS database and a Jupyter notebook container wired together, passing ACLED credentials via a `.env` file and mounting the repository for reproducible analysis environments.
* `pyproject.toml` defines the installable package metadata, while `requirements.txt` captures optional extras for geospatial processing, optimization, dashboarding, and PDF export, making the stack flexible for local development versus CI usage.

### Quality & testing

* Lightweight unit tests verify key math utilities: the Haversine implementation, the distance matrix/nearest-neighbor heuristic, and the Bayesian event rate calculator, ensuring core algorithms behave as expected before integrating with heavier data pulls.
* The quick-start steps direct contributors to install the package in editable mode and run `pytest`, aligning with the scripted helpers for end-to-end validation.

### Working with the project

A typical workflow involves launching the Docker stack, pulling fresh ACLED and HDX data via the provided scripts, enriching spatial tables, recomputing Bayesian risk layers, and then exploring situational awareness through the Streamlit dashboard or exported maps. The modular package design and command-line wrappers make it straightforward to automate updates in CI/CD or rerun analyses offline, while the tests and logging utilities help diagnose data quality or connectivity issues quickly.

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
