#!/usr/bin/env python3
"""Pull ACLED events for one or more countries or regions.

This module fetches recent ACLED conflict events, saves a CSV backup and
writes the results to PostGIS. It can be used as a library or run from the
command line.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from requests.exceptions import RequestException
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from ..utils.logging import setup_logging

# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------
logger = setup_logging(__name__)

# ---------------------------------------------------------------------------
# constants
# ---------------------------------------------------------------------------
ISO_CACHE = Path("data/meta/iso_cache.csv")
DEFAULT_DB_URL = "postgresql://postgres:postgres@db:5432/pathfinder"
DAYS_BACK = 14

# region aliases (add more if desired)
REGION_ALIASES: Dict[str, int] = {
    "western_africa": 1,
    "middle_africa": 2,
    "eastern_africa": 3,
    "southern_africa": 4,
    "northern_africa": 5,
    "south_asia": 7,
    "southeast_asia": 9,
    "middle_east": 11,
    "europe": 12,
    "caucasus_central_asia": 13,
    "central_america": 14,
    "south_america": 15,
    "caribbean": 16,
    "east_asia": 17,
    "north_america": 18,
    "oceania": 19,
    "antarctica": 20,
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def load_iso_table(token: str, email: str) -> pd.DataFrame:
    """Return a dataframe of ISO codes, caching the result for a day."""
    try:
        if ISO_CACHE.exists() and ISO_CACHE.stat().st_mtime > time.time() - 86400:
            df = pd.read_csv(ISO_CACHE)
            logger.debug("Loaded ISO cache with %d rows", len(df))
        else:
            logger.info("Downloading country list from ACLED …")
            url = (
                "https://api.acleddata.com/country/read?"
                f"key={token}&email={email}&limit=0&format=json"
            )
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            df = pd.DataFrame(resp.json().get("data", []))["country iso iso3".split()]  # TODO: confirm field names
            ISO_CACHE.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(ISO_CACHE, index=False)
            logger.debug("Saved ISO cache → %s", ISO_CACHE)
    except (RequestException, ValueError) as exc:
        logger.error("Failed to fetch ISO table: %s", exc)
        raise
    return df


def build_queries(items: Iterable[str], lookup: Dict[str, str], aliases: Dict[str, int]) -> Tuple[str, str, List[str]]:
    """Build ISO and region query strings for the API."""
    iso_params: List[str] = []
    region_params: List[str] = []
    missing: List[str] = []
    for item in items:
        if item in lookup:
            iso_params.append(f"&iso={lookup[item]}")
        elif item in aliases:
            region_params.append(f"&region={aliases[item]}")
        else:
            missing.append(item)
    return "".join(iso_params), "".join(region_params), missing


def fetch_acled(token: str, email: str, iso_query: str, region_query: str,
                days_back: int = DAYS_BACK) -> pd.DataFrame:
    """Fetch ACLED events within ``days_back`` days."""
    today = dt.date.today()
    start_date = today - dt.timedelta(days=days_back)
    url = (
        "https://api.acleddata.com/acled/read?"
        f"email={email}&key={token}"
        f"{iso_query}{region_query}"
        f"&event_date={start_date}|{today}"  # DEBUG TIP: If this fails, check the ACLED_TOKEN in your .env file
        "&limit=20000&format=json"
    )
    try:
        logger.info("Fetching ACLED: %s", url)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        js = resp.json()
        rows = js.get("data", [])
        if not rows:
            raise ValueError("Query succeeded but returned zero rows")
        df = pd.DataFrame(rows)
        logger.debug("Fetched %d rows", len(df))
        return df
    except RequestException as exc:
        logger.error("ACLED request failed: %s", exc)
        raise
    except ValueError as exc:
        logger.error("ACLED response error: %s", exc)
        raise


def save_csv(df: pd.DataFrame, countries: Iterable[str]) -> Path:
    """Write dataframe to ``data/raw`` and return the path."""
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_csv = raw_dir / f"acled_{'_'.join(countries)}_{dt.date.today()}.csv"
    df.to_csv(out_csv, index=False)
    logger.info("Saved %d rows → %s", len(df), out_csv)
    return out_csv


def write_postgis(df: pd.DataFrame, table: str = "events_raw",
                  db_url: str = DEFAULT_DB_URL) -> None:
    """Write dataframe to a PostGIS table."""
    try:
        engine = create_engine(db_url)
        engine.connect().close()
    except SQLAlchemyError as exc:
        logger.error("Database connection failed: %s", exc)
        raise
    try:
        df.to_sql(table, engine, if_exists="replace", index=False)
        logger.info("Written to PostGIS table %s", table)
    except SQLAlchemyError as exc:
        logger.error("Failed writing to PostGIS: %s", exc)
        raise


def list_countries(token: str, email: str) -> List[str]:
    """Return list of valid country names from ACLED."""
    df = load_iso_table(token, email)
    return sorted(df["country"].tolist())


# ---------------------------------------------------------------------------
# main entry point
# ---------------------------------------------------------------------------

def main(argv: Iterable[str] | None = None) -> None:
    """Entry point for CLI usage."""
    args = list(argv) if argv is not None else sys.argv[1:]

    if "--help" in args or "-h" in args:
        print(__doc__)
        return
    list_mode = "--list" in args
    args = [a for a in args if a != "--list"]

    token = os.getenv("ACLED_TOKEN")
    email = os.getenv("ACLED_EMAIL")
    if not token or not email:
        logger.error("ACLED_TOKEN and ACLED_EMAIL environment variables must be set")
        sys.exit(1)

    if list_mode:
        for name in list_countries(token, email):
            print(name)
        return

    if not args:
        logger.error("No country or region arguments supplied. Use --list to see options")
        sys.exit(1)

    try:
        iso_df = load_iso_table(token, email)
        lookup = iso_df.assign(iso=iso_df["iso"].astype(str)).set_index("country")["iso"].to_dict()
        iso_q, region_q, missing = build_queries(args, lookup, REGION_ALIASES)
        if missing:
            logger.error("Unrecognised names: %s", missing)
            sys.exit(1)
        df = fetch_acled(token, email, iso_q, region_q)
        save_csv(df, args)
        write_postgis(df)
    except Exception as exc:
        logger.error("Script failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()

# DEBUG QUESTIONS:
# 1. What happens if ACLED returns an empty payload?
# 2. Does the PostGIS table already exist before this script writes to it?
# 3. How might this fail when run in a CI pipeline?
