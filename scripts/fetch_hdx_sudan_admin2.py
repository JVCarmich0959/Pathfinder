#!/usr/bin/env python3
"""Fetch the latest Sudan Admin2 monthly CSV from HDX and load into PostGIS."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import requests
from requests import RequestException

from pathfinder.etl.sudan_admin2_monthly import (
    load_admin2_monthly_csv,
    read_metadata,
    write_metadata,
)

LOGGER = logging.getLogger("pathfinder.scripts.fetch_hdx_sudan_admin2")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

HDX_API = "https://data.humdata.org/api/3/action/package_show"
DEFAULT_DATASET = "sudan-acled-conflict-data"
DEFAULT_DEST = Path("data/raw/sudan_admin2_monthly.csv")
RESOURCE_SUBSTRING = "Sudan_PV - Data.csv"
TIMEOUT = 60


def fetch_latest_csv(dataset: str, dest: Path) -> Optional[Path]:
    """Return the path to the downloaded CSV, or ``None`` if already current."""
    params = {"id": dataset}
    LOGGER.info("fetching package metadata", extra={"dataset": dataset})
    try:
        response = requests.get(HDX_API, params=params, timeout=TIMEOUT)
        response.raise_for_status()
    except RequestException as exc:
        if Path(dest).exists():
            LOGGER.warning(
                "hdx request failed; using cached file",
                extra={"path": str(dest), "error": str(exc)},
            )
            return dest
        raise

    package = response.json()["result"]

    resource = next(
        (
            r
            for r in package["resources"]
            if r.get("url", "").lower().endswith(".csv")
            and RESOURCE_SUBSTRING.lower() in r.get("name", "").lower()
        ),
        None,
    )
    if resource is None:
        resource = next(
            (r for r in package["resources"] if r.get("format", "").lower() == "csv"),
            None,
        )
    if resource is None:
        raise RuntimeError("Could not locate Sudan Admin2 CSV resource in HDX package")

    metadata = read_metadata()
    last_modified = resource.get("last_modified") or resource.get("revision_timestamp")
    if metadata.get("last_modified") == last_modified and Path(dest).exists():
        LOGGER.info("local copy already current", extra={"path": str(dest)})
        return None

    dest.parent.mkdir(parents=True, exist_ok=True)
    url = resource["url"]
    LOGGER.info("downloading", extra={"url": url, "dest": str(dest)})

    with requests.get(url, stream=True, timeout=TIMEOUT) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                fh.write(chunk)

    write_metadata({
        "dataset": dataset,
        "resource_id": resource.get("id"),
        "last_modified": last_modified,
        "download_url": url,
    })
    LOGGER.info("downloaded", extra={"bytes": dest.stat().st_size})
    return dest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="HDX dataset slug (default: %(default)s)")
    parser.add_argument("--dest", default=str(DEFAULT_DEST), help="Where to save the CSV (default: %(default)s)")
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Only download the CSV; do not load into PostGIS",
    )
    args = parser.parse_args()

    dest_path = fetch_latest_csv(args.dataset, Path(args.dest))
    if dest_path is None:
        dest_path = Path(args.dest)

    if args.skip_load:
        LOGGER.info("skipping database load by request")
        return

    load_admin2_monthly_csv(dest_path)


if __name__ == "__main__":
    main()
