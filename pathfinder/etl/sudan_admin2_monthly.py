"""Utilities for loading Sudan monthly political-violence aggregates."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, text

from pathfinder.db import get_engine

LOGGER = logging.getLogger(__name__)


REQUIRED_COLUMNS = {
    "country",
    "admin1",
    "admin2",
    "iso3",
    "admin2_pcode",
    "admin1_pcode",
    "month",
    "year",
    "events",
    "fatalities",
}


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return dataframe with snake_case columns."""
    renamed = df.copy()
    renamed.columns = [c.strip().lower().replace(" ", "_") for c in renamed.columns]
    return renamed


def transform_admin2_monthly(raw: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw HDX CSV and return Admin2-level monthly metrics.

    Parameters
    ----------
    raw:
        Raw dataframe as provided by HDX (Admin2 rows).

    Returns
    -------
    pd.DataFrame
        Columns: country, iso3, admin1, admin1_pcode, admin2, admin2_pcode,
        year, month (numeric), events, fatalities.
    """

    df = _normalise_columns(raw)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        msg = f"CSV is missing required columns: {sorted(missing)}"
        LOGGER.error(msg)
        raise ValueError(msg)

    tidy = df.loc[:, [
        "country",
        "iso3",
        "admin1",
        "admin1_pcode",
        "admin2",
        "admin2_pcode",
        "month",
        "year",
        "events",
        "fatalities",
    ]].copy()

    tidy["month"] = tidy["month"].astype(str).str.strip()
    tidy["year"] = tidy["year"].astype(int)

    # Combine month name with year, convert to a numeric month (1-12)
    month_dt = pd.to_datetime(
        tidy["month"].str[:3] + " " + tidy["year"].astype(str),
        format="%b %Y",
        errors="coerce",
    )
    if month_dt.isna().any():
        bad_rows = tidy.loc[month_dt.isna(), ["month", "year"]]
        msg = f"Unparseable month/year combinations: {bad_rows.to_dict(orient='records')}"
        LOGGER.error(msg)
        raise ValueError(msg)
    tidy["month"] = month_dt.dt.month.astype(int)

    tidy["iso3"] = tidy["iso3"].astype(str).str.upper().str.strip()
    tidy["country"] = tidy["country"].astype(str).str.strip()
    tidy["admin1"] = tidy["admin1"].astype(str).str.strip()
    tidy["admin2"] = tidy["admin2"].astype(str).str.strip()
    tidy["admin1_pcode"] = tidy["admin1_pcode"].astype(str).str.strip()
    tidy["admin2_pcode"] = tidy["admin2_pcode"].astype(str).str.strip()

    tidy["events"] = tidy["events"].fillna(0).astype(int)
    tidy["fatalities"] = tidy["fatalities"].fillna(0).astype(int)

    tidy = tidy.sort_values(["iso3", "admin1_pcode", "admin2_pcode", "year", "month"]).reset_index(drop=True)
    return tidy


def aggregate_country_monthly(admin2_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate Admin2 metrics into monthly country totals."""
    grouped = (
        admin2_df.groupby(["iso3", "year", "month"], as_index=False)[["events", "fatalities"]]
        .sum()
        .rename(columns={"iso3": "iso"})
        .sort_values(["iso", "year", "month"])
        .reset_index(drop=True)
    )
    return grouped


def load_admin2_monthly_csv(csv_path: Path, engine: Engine | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the Sudan Admin2 monthly CSV into PostGIS.

    Parameters
    ----------
    csv_path:
        Local path to the HDX CSV snapshot.
    engine:
        Optional SQLAlchemy engine. If omitted, :func:`pathfinder.db.get_engine`
        is used.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        (admin2_df, monthly_df) dataframes used for persistence.
    """

    if engine is None:
        engine = get_engine()

    csv_path = Path(csv_path)
    if not csv_path.exists():
        msg = f"CSV path does not exist: {csv_path}"
        LOGGER.error(msg)
        raise FileNotFoundError(msg)

    LOGGER.info("reading csv", extra={"path": str(csv_path)})
    raw = pd.read_csv(csv_path)

    admin2_df = transform_admin2_monthly(raw)
    monthly_df = aggregate_country_monthly(admin2_df)

    LOGGER.info(
        "writing to postgis",
        extra={
            "admin2_rows": len(admin2_df),
            "monthly_rows": len(monthly_df),
        },
    )

    with engine.begin() as conn:
        admin2_df.to_sql("sudan_admin2_monthly", conn, if_exists="replace", index=False)
        monthly_df.to_sql("sudan_monthly_violence", conn, if_exists="replace", index=False)

        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_sudan_admin2_monthly_pcode_month "
                "ON sudan_admin2_monthly(admin2_pcode, year, month)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_sudan_monthly_violence_iso_month "
                "ON sudan_monthly_violence(iso, year, month)"
            )
        )

    return admin2_df, monthly_df


METADATA_PATH = Path("data/raw/sudan_admin2_monthly.meta.json")


def write_metadata(metadata: dict) -> None:
    METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    METADATA_PATH.write_text(json.dumps(metadata, indent=2, sort_keys=True))


def read_metadata() -> dict:
    if not METADATA_PATH.exists():
        return {}
    return json.loads(METADATA_PATH.read_text())
