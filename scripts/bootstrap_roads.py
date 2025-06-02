#!/usr/bin/env python3
"""Initialise road tables if missing."""

from pathlib import Path
import logging
import sqlalchemy as sa
from pathfinder.settings import engine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    eng = engine()
    insp = sa.inspect(eng)
    if insp.has_table("roads_primary"):
        logger.info("roads_primary table already exists")
        return
    try:
        sql = Path("sql/01_normalize_road_layers.sql").read_text()
        with eng.begin() as conn:
            conn.exec_driver_sql(sql)
        logger.info("Bootstrapped road schema")
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to bootstrap road schema: %s", exc)
        raise


if __name__ == "__main__":
    main()
