"""Aggregate raw ACLED events into monthly country totals."""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from ..db import get_engine
from ..utils.logging import setup_logging

logger = setup_logging(__name__)

DEFAULT_SOURCE_TABLE = "events_raw"
DEFAULT_DEST_TABLE = "sa_monthly_violence"
TMP_TABLE = "_sa_monthly_violence_tmp"
REQUIRED_EVENT_COLUMNS = {"iso", "event_date"}

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Explicit SQLAlchemy dtypes so we can create empty tables deterministically.
TO_SQL_DTYPES = {
    "iso": sa.String(3),
    "country": sa.String(128),
    "year": sa.Integer(),
    "month": sa.Integer(),
    "events": sa.Integer(),
    "fatalities": sa.Integer(),
}


def validate_identifier(name: str) -> str:
    """Validate ``name`` for use as a plain SQL identifier (optionally schema-qualified)."""

    if not name:
        raise ValueError("Identifier must be a non-empty string")

    parts = name.split(".")
    if len(parts) > 2:
        raise ValueError("Only simple or schema-qualified identifiers are supported")

    for part in parts:
        if not IDENTIFIER_PATTERN.fullmatch(part):
            raise ValueError(
                "Invalid identifier '%s'; only alphanumerics and underscores allowed" % name
            )
    return name


def split_identifier(name: str) -> tuple[Optional[str], str]:
    """Return ``(schema, table)`` for ``name`` after validation."""

    validated = validate_identifier(name)
    parts = validated.split(".")
    if len(parts) == 1:
        return None, parts[0]
    return parts[0], parts[1]


def qualify_identifier(name: str) -> str:
    """Return a safe SQL fragment for ``name``."""

    schema, table = split_identifier(name)
    return f"{schema}.{table}" if schema else table


def ensure_table_exists(engine: Engine, table_name: str) -> None:
    """Raise ``RuntimeError`` if ``table_name`` is missing."""
    schema, table = split_identifier(table_name)
    inspector = inspect(engine)
    if not inspector.has_table(table, schema=schema):
        raise RuntimeError(f"Required table '{table_name}' is missing")


def load_events_from_csv(csv_path: str | Path) -> pd.DataFrame:
    """Load events from a CSV snapshot for offline dry-runs."""

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV snapshot {path} does not exist")

    logger.info("Reading events from CSV snapshot %s", path)
    events = pd.read_csv(path)
    missing = REQUIRED_EVENT_COLUMNS - set(events.columns)
    if missing:
        raise ValueError(
            "CSV snapshot is missing required columns: %s" % ", ".join(sorted(missing))
        )
    return events


def fetch_events(engine: Engine, source_table: str) -> pd.DataFrame:
    """Fetch minimal event columns needed for aggregation."""
    table = qualify_identifier(source_table)
    query = text(
        f"""
        SELECT iso, country, event_date, fatalities
        FROM {table}
        WHERE event_date IS NOT NULL AND iso IS NOT NULL
        """
    )
    logger.info("Reading events from %s", table)
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
    schema, dest_table = split_identifier(destination_table)
    dest_qualified = qualify_identifier(destination_table)
    tmp_table = validate_identifier(TMP_TABLE)
    tmp_qualified = f"{schema}.{tmp_table}" if schema else tmp_table

    if monthly.empty:
        logger.info(
            "Monthly aggregates are empty; destination table %s will be truncated",
            dest_qualified,
        )
        monthly_to_write = monthly.copy()
    else:
        monthly_to_write = monthly

    with engine.begin() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {tmp_qualified};")
        monthly_to_write.to_sql(
            tmp_table,
            conn,
            schema=schema,
            if_exists="replace",
            index=False,
            method="multi",
            dtype=TO_SQL_DTYPES,
        )
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {dest_qualified};")
        conn.exec_driver_sql(f"ALTER TABLE {tmp_qualified} RENAME TO {dest_table};")
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS {dest_table}_iso_year_month_idx "
            f"ON {dest_qualified} (iso, year, month);"
        )
        conn.exec_driver_sql(
            f"CREATE INDEX IF NOT EXISTS {dest_table}_year_month_idx "
            f"ON {dest_qualified} (year, month);"
        )
    logger.info("Wrote %d rows to %s", len(monthly_to_write), dest_qualified)


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
        "--source-csv",
        default=None,
        help="Optional CSV snapshot path for offline aggregation; overrides --source-table",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute aggregates without writing to PostGIS",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    source_table = validate_identifier(args.source_table)
    destination_table = validate_identifier(args.destination_table)
    engine: Engine | None = None

    if args.source_csv:
        events = load_events_from_csv(args.source_csv)
    else:
        engine = sa.create_engine(args.database_url) if args.database_url else get_engine()
        ensure_table_exists(engine, source_table)
        events = fetch_events(engine, source_table)
    monthly = aggregate_events_dataframe(events)

    if args.dry_run:
        logger.info("Dry run requested; first rows:\n%s", monthly.head())
        return

    if engine is None:
        engine = sa.create_engine(args.database_url) if args.database_url else get_engine()

    write_monthly_table(engine, monthly, destination_table=destination_table)


if __name__ == "__main__":  # pragma: no cover
    main()
