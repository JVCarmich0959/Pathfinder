"""Aggregate raw ACLED events into monthly country totals."""
from __future__ import annotations

import argparse
from typing import Iterable, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text

from ..db import get_engine
from ..utils.logging import setup_logging

logger = setup_logging(__name__)

DEFAULT_SOURCE_TABLE = "events_raw"
DEFAULT_DEST_TABLE = "sa_monthly_violence"
TMP_TABLE = "_sa_monthly_violence_tmp"


def ensure_table_exists(engine: Engine, table_name: str) -> None:
    """Raise ``RuntimeError`` if ``table_name`` is missing."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        raise RuntimeError(f"Required table '{table_name}' is missing")


def fetch_events(engine: Engine, source_table: str) -> pd.DataFrame:
    """Fetch minimal event columns needed for aggregation."""
    query = text(
        f"""
        SELECT iso, country, event_date, fatalities
        FROM {source_table}
        WHERE event_date IS NOT NULL AND iso IS NOT NULL
        """
    )
    logger.info("Reading events from %s", source_table)
    return pd.read_sql_query(query, engine)


def aggregate_events_dataframe(events: pd.DataFrame) -> pd.DataFrame:
    """Aggregate event DataFrame to monthly country totals."""
    base = pd.DataFrame(
        {
            "iso": pd.Series(dtype="string"),
            "country": pd.Series(dtype="string"),
            "year": pd.Series(dtype="int64"),
            "month": pd.Series(dtype="int64"),
            "events": pd.Series(dtype="int64"),
            "fatalities": pd.Series(dtype="int64"),
        }
    )

    if events.empty:
        logger.warning("No events supplied; returning empty aggregate")
        return base

    frame = events.copy()
    frame["event_date"] = pd.to_datetime(frame["event_date"], errors="coerce")
    frame = frame.dropna(subset=["event_date", "iso"])

    if frame.empty:
        logger.warning("All rows dropped after cleaning; returning empty aggregate")
        return base

    frame["iso"] = frame["iso"].astype("string").str.strip().str.upper()
    frame["country"] = frame.get("country", pd.Series(dtype="string")).fillna(frame["iso"]).astype("string").str.strip()
    frame["year"] = frame["event_date"].dt.year.astype("int64")
    frame["month"] = frame["event_date"].dt.month.astype("int64")
    frame["fatalities"] = (
        pd.to_numeric(frame.get("fatalities"), errors="coerce")
        .fillna(0)
        .astype("int64")
    )

    aggregated = (
        frame.groupby(["iso", "country", "year", "month"], dropna=False)
        .agg(events=("iso", "size"), fatalities=("fatalities", "sum"))
        .reset_index()
        .sort_values(["iso", "year", "month"])  # deterministic order
    )
    aggregated["events"] = aggregated["events"].astype("int64")
    aggregated["fatalities"] = aggregated["fatalities"].astype("int64")

    logger.info(
        "Aggregated %d monthly rows from %d input events",
        len(aggregated),
        len(frame),
    )
    return aggregated


def write_monthly_table(
    engine: Engine,
    monthly: pd.DataFrame,
    destination_table: str = DEFAULT_DEST_TABLE,
) -> None:
    """Replace ``destination_table`` with aggregated data atomically."""
    if monthly.empty:
        logger.info("No aggregates to write; skipping PostGIS update")
        return

    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {TMP_TABLE};")
        monthly.to_sql(TMP_TABLE, conn, if_exists="replace", index=False, method="multi")
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {destination_table};")
        conn.exec_driver_sql(f"ALTER TABLE {TMP_TABLE} RENAME TO {destination_table};")
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS {destination_table}_iso_year_month_idx "
            f"ON {destination_table} (iso, year, month);"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS {destination_table}_year_month_idx "
            f"ON {destination_table} (year, month);"
        )
    logger.info("Wrote %d rows to %s", len(monthly), destination_table)


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-table",
        default=DEFAULT_SOURCE_TABLE,
        help="Source table containing raw events (default: events_raw)",
    )
    parser.add_argument(
        "--destination-table",
        default=DEFAULT_DEST_TABLE,
        help="Destination table for monthly aggregates (default: sa_monthly_violence)",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional SQLAlchemy database URL; falls back to get_engine()",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute aggregates without writing to PostGIS",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    engine = sa.create_engine(args.database_url) if args.database_url else get_engine()

    ensure_table_exists(engine, args.source_table)
    events = fetch_events(engine, args.source_table)
    monthly = aggregate_events_dataframe(events)

    if args.dry_run:
        logger.info("Dry run requested; first rows:\n%s", monthly.head())
        return

    write_monthly_table(engine, monthly, destination_table=args.destination_table)


if __name__ == "__main__":  # pragma: no cover
    main()
