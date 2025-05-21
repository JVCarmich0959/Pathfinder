#!/usr/bin/env python3
"""Load admin boundaries and enrich monthly ACLED data."""
import os
import subprocess
from pathlib import Path
import sqlalchemy as sa

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/pathfinder",
)
engine = sa.create_engine(DB_URL)

# ensure PostGIS extension
with engine.begin() as conn:
    conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")

# path to admin2 shapefile
shp = Path("data/geo/sudan_admin2.shp")
if not shp.exists():
    raise SystemExit(f"Missing {shp}")

# load shapefile via ogr2ogr
url = sa.engine.URL.create(DB_URL)
ogr_cmd = [
    "ogr2ogr", "-f", "PostgreSQL",
    f"PG:dbname={url.database} host={url.host} user={url.username} password={url.password}",
    str(shp), "-nln", "geo_admin2", "-nlt", "MULTIPOLYGON", "-overwrite",
]
subprocess.run(ogr_cmd, check=True)

SQL = """
ALTER TABLE IF EXISTS acled_monthly_clean
    ADD COLUMN IF NOT EXISTS geom geometry(Point,4326);

UPDATE acled_monthly_clean
SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
WHERE geom IS NULL;

DROP TABLE IF EXISTS acled_monthly_enriched;
CREATE TABLE acled_monthly_enriched AS
SELECT a.*, g.admin2_name, g.admin1_name
FROM acled_monthly_clean a
LEFT JOIN geo_admin2 g
  ON ST_Contains(g.geom, a.geom);

CREATE INDEX IF NOT EXISTS acled_monthly_enriched_geom_idx
  ON acled_monthly_enriched USING GIST(geom);
"""

with engine.begin() as conn:
    conn.exec_driver_sql(SQL)

print("\u2705  acled_monthly_enriched created")
