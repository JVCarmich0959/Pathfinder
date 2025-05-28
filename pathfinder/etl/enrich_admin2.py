#!/usr/bin/env python3
"""Load admin boundaries and enrich monthly ACLED data."""

import os
import subprocess
from pathlib import Path
from typing import Iterable

import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from ..utils.logging import setup_logging

logger = setup_logging(__name__)

DEFAULT_DB_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@db:5432/pathfinder"
)


def ensure_postgis(engine: sa.Engine) -> None:
    """Ensure the PostGIS extension exists."""
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS postgis;")


def load_admin2(shp: Path, db_url: str = DEFAULT_DB_URL) -> None:
    """Load the admin2 shapefile into PostGIS via ogr2ogr."""
    if not shp.exists():
        raise FileNotFoundError(shp)
    url = sa.engine.URL.create(db_url)
    ogr_cmd = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        f"PG:dbname={url.database} host={url.host} user={url.username} password={url.password}",
        str(shp),
        "-nln",
        "geo_admin2",
        "-nlt",
        "MULTIPOLYGON",
        "-overwrite",
    ]
    try:
        subprocess.run(ogr_cmd, check=True)
        logger.info("Loaded %s into geo_admin2", shp)
    except subprocess.CalledProcessError as exc:
        logger.error("ogr2ogr failed: %s", exc)
        raise


def enrich_monthly(engine: sa.Engine) -> None:
    """Add admin names to monthly ACLED table."""
    sql = """
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
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
        logger.info("acled_monthly_enriched created")
    except SQLAlchemyError as exc:
        logger.error("Failed to run enrichment SQL: %s", exc)
        raise


def main(argv: Iterable[str] | None = None) -> None:
    """CLI entry point."""
    shp = Path("data/geo/sudan_admin2.shp")
    engine = sa.create_engine(DEFAULT_DB_URL)
    try:
        ensure_postgis(engine)
        load_admin2(shp, DEFAULT_DB_URL)
        enrich_monthly(engine)
    except Exception as exc:
        logger.error("Script failed: %s", exc)
        raise


if __name__ == "__main__":  # pragma: no cover
    main()

# DEBUG QUESTIONS:
# 1. Does the PostGIS table geo_admin2 already exist before loading?
# 2. What happens if ogr2ogr cannot access the shapefile path?
# 3. How might this fail when run in a CI pipeline?
