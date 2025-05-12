#!/usr/bin/env python3
"""
Download the South-Africa monthly-aggregate XLSX from HDX,
convert to tidy CSV, and load into PostGIS.
Usage:
    python scripts/fetch_hdx_sa_monthly.py \
        "https://data.humdata.org/…/south-africa_political_violence_events_and_fatalities_by_month-year_as-of-08may2025.xlsx"
"""

import sys, requests, pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

if len(sys.argv) != 2:
    sys.exit("Pass the HDX XLSX URL as the only argument.")
HDX_XLSX = sys.argv[1]

# ---------------- 1. download ----------------
out_dir = Path("data/raw")
out_dir.mkdir(parents=True, exist_ok=True)
xlsx_path = out_dir / "sa_monthly_violence.xlsx"

print("⬇️  Downloading HDX XLSX …")
with requests.get(HDX_XLSX, stream=True, timeout=120) as r:
    r.raise_for_status()
    with open(xlsx_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

# ---------------- 2. tidy ----------------
print("📖  Reading workbook …")
df0 = pd.read_excel(xlsx_path, sheet_name=0)

# Expect columns like: ['year', 'month', 'events', 'fatalities']
df = (
    df0.rename(columns=str.lower)
       .assign(
           year=lambda d: d['year'].astype(int),
           month=lambda d: d['month'].astype(int)
       )
       .sort_values(['year', 'month'])
)

csv_path = out_dir / "sa_monthly_violence.csv"
df.to_csv(csv_path, index=False)
print(f"💾  Saved tidy CSV → {csv_path}")

# ---------------- 3. PostGIS ----------------
pg = "postgresql://postgres:postgres@db:5432/pathfinder"
engine = create_engine(pg)
df.to_sql("sa_monthly_violence", engine, if_exists="replace", index=False)
print("✅  Written to PostGIS table sa_monthly_violence")
