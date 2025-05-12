# Pathfinder

Risk-aware, offline-friendly maps and routing tools for Sudan–Chad
humanitarian operations. Pathfinder fuses fresh satellite imagery,
OpenStreetMap edits, and conflict-event data to publish daily safe-route
layers (GeoJSON, MBTiles, printable PDFs) for refugees, local drivers,
and aid convoys.
flowchart TD
  %% ───────────────────────  STACK  ───────────────────────
  subgraph STACK [Docker Stack]
    D(PostGIS 16 + PostGIS):::db
    N(Jupyter SciPy‐Notebook):::nb
  end

  %% ───────────────────────  DATA INGESTION  ─────────────────────
  style STACK fill:#fff0,stroke-width:0px;
  D & N -- docker-compose.yml --> Z[Running Services]

  subgraph INGESTION [Data Ingestion]
    A1[ACLED API pull<br>scripts/pull_acled.py]:::scr
    A2[HDX XLSX ingest<br>scripts/fetch_hdx_sa_monthly.py]:::scr
  end
  A1 & A2 -->|events_raw<br>sa_monthly_violence| D

  %% ───────────────────────  PROCESSING  ────────────────────────
  subgraph PROCESSING [Processing & Risk Model]
    R1[Risk Raster / Edge Cost<br>(SQL + Python)]:::proc
    R2[Pairwise LCP Matrix<br>postgis + st_shortestpath]:::proc
    R3[TSP Solver<br>(OR-Tools GLS)]:::proc
  end
  D --> R1 --> R2 --> R3

  %% ───────────────────────  OUTPUT  ────────────────────────────
  subgraph OUTPUT [Outputs]
    V1[Risk Map Notebook<br>quickmap.ipynb]:::nb
    V2[Route Notebook<br>route_tsp.ipynb]:::nb
    X1[GeoJSON / MBTiles<br>for field GPS]:::out
  end
  R1 --> V1
  R3 -->|best tour| V2 --> X1

  %% ───────────────────────  AUTOMATION  ───────────────────────
  subgraph CI [Automation]
    C1[GitHub Actions:<br>nightly data refresh & tests]:::ci
  end
  A1 & A2 & R1 & R2 & R3 --> C1

  %% style blocks
  classDef db fill:#c6e0ff,stroke:#2c69c6,color:#003f7f;
  classDef nb fill:#fbe9cf,stroke:#c27b00,color:#4c2d00;
  classDef scr fill:#e2ffe2,stroke:#00a000,color:#006400;
  classDef proc fill:#ffe5f2,stroke:#c61b63,color:#7a0036;
  classDef out fill:#dadada,stroke:#707070;
  classDef ci fill:#e8d8ff,stroke:#6b46c1,color:#382175;
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


