#!/usr/bin/env python3
"""Initialise road tables if missing."""

from pathlib import Path
import logging
import sqlalchemy as sa
from sqlalchemy import text
from pathfinder.settings import engine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def execute_sql_file(engine: sa.Engine, sql_file_path: Path) -> None:
    """Execute a SQL file using SQLAlchemy's text() wrapper."""

    sql = sql_file_path.read_text()
    with engine.begin() as conn:
        conn.execute(text(sql))


def main() -> None:
    eng = engine()
    insp = sa.inspect(eng)
    if insp.has_table("roads_primary"):
        logger.info("roads_primary table already exists")
        return
    try:
        execute_sql_file(eng, Path("sql/01_normalize_road_layers.sql"))
        logger.info("Bootstrapped road schema")
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to bootstrap road schema: %s", exc)
        raise


if __name__ == "__main__":
    main()
