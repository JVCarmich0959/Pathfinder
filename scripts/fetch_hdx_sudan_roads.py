#!/usr/bin/env python3
"""
Fetch Sudan OSM roads (HOT export) → tidy → PostGIS

Usage
-----
python scripts/fetch_hdx_sudan_roads.py \
  https://s3.dualstack.us-east-1.amazonaws.com/production-raw-data-api/ISO3/SDN/roads/lines/hotosm_sdn_roads_lines_shp.zip
"""

from pathlib import Path
import sys, requests, geopandas as gpd
from sqlalchemy import create_engine

# ------------------------------------------------------------------ CLI & paths
if len(sys.argv) != 2:
    sys.exit("Pass the ZIP URL as the single argument.")
URL = sys.argv[1]

raw_dir  = Path("data/raw")
raw_dir.mkdir(parents=True, exist_ok=True)
zip_path = raw_dir / "sudan_roads.zip"

# ------------------------------------------------------------------ 1 ▸ download
print("⬇️  Downloading roads ZIP …")
with requests.get(URL, stream=True, timeout=120) as r:
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

# ------------------------------------------------------------------ 2 ▸ read directly from the archive
print("📖  Opening archive via GDAL …")
gdf = gpd.read_file(f"/vsizip/{zip_path}")          # ← magic happens here
print(f"✅  {len(gdf):,} features read")

# ------------------------------------------------------------------ 3 ▸ keep a GPKG backup
gpkg_path = raw_dir / "sudan_roads.gpkg"
gdf.to_file(gpkg_path, driver="GPKG")
print(f"💾  Saved GeoPackage → {gpkg_path}")

# ------------------------------------------------------------------ 4 ▸ write to PostGIS
pg_url = "postgresql://postgres:postgres@db:5432/pathfinder"
engine = create_engine(pg_url)
gdf.to_postgis("sudan_roads_osm", engine, if_exists="replace", index=False)
print("🗄️  Written to PostGIS table sudan_roads_osm")
print("✅  Done")