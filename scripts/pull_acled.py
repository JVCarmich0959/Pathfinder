#!/usr/bin/env python3
"""
Pathfinder – universal ACLED puller
-----------------------------------
Usage examples
$ python scripts/pull_acled.py Sudan Chad
$ python scripts/pull_acled.py northern_africa
$ python scripts/pull_acled.py --list            # show valid names
"""

from __future__ import annotations
from pathlib import Path
import os, sys, time, datetime as dt, requests, pandas as pd
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# 0. CLI parsing
# ---------------------------------------------------------------------------
args = sys.argv[1:]
if "--help" in args or "-h" in args:
    print(__doc__)
    sys.exit(0)
LIST_MODE = "--list" in args
args = [a for a in args if a != "--list"]

# ---------------------------------------------------------------------------
# 1. ENV – token & email
# ---------------------------------------------------------------------------
TOKEN = os.getenv("ACLED_TOKEN")
EMAIL = os.getenv("ACLED_EMAIL")
if not TOKEN or not EMAIL:
    sys.exit("\n❌  ACLED_TOKEN and/or ACLED_EMAIL missing in environment.\n")

# ---------------------------------------------------------------------------
# 2. Fetch / cache ISO table
# ---------------------------------------------------------------------------
CACHE = Path("data/meta/iso_cache.csv")
CACHE.parent.mkdir(parents=True, exist_ok=True)
if CACHE.exists() and CACHE.stat().st_mtime > time.time() - 86400:
    iso_df = pd.read_csv(CACHE)
else:
    print("🔄  Downloading country list from ACLED …")
    url_iso = f"https://api.acleddata.com/country/read?key={TOKEN}&email={EMAIL}&limit=0&format=json"
    resp = requests.get(url_iso, timeout=30)
    resp.raise_for_status()
    iso_df = pd.DataFrame(resp.json()["data"])[["country", "iso", "iso3"]]
    iso_df.to_csv(CACHE, index=False)

country_lookup = (
    iso_df.assign(iso=lambda d: d["iso"].astype(str))
          .set_index("country", drop=True)["iso"]
          .to_dict()
)

# region aliases (add more if desired)
REGION_ALIASES: dict[str, int] = {
    "western_africa": 1, "middle_africa": 2, "eastern_africa": 3,
    "southern_africa": 4, "northern_africa": 5, "south_asia": 7,
    "southeast_asia": 9, "middle_east": 11, "europe": 12,
    "caucasus_central_asia": 13, "central_america": 14, "south_america": 15,
    "caribbean": 16, "east_asia": 17, "north_america": 18,
    "oceania": 19, "antarctica": 20,
}

if LIST_MODE:
    print("\n".join(sorted(country_lookup.keys())))
    sys.exit(0)

if not args:
    sys.exit("❌  No country or region arguments supplied.  Use --list to see options.")

# ---------------------------------------------------------------------------
# 3. Build query parts
# ---------------------------------------------------------------------------
iso_params, region_params, missing = [], [], []
for item in args:
    if item in country_lookup:
        iso_params.append(f"&iso={country_lookup[item]}")
    elif item in REGION_ALIASES:
        region_params.append(f"&region={REGION_ALIASES[item]}")
    else:
        missing.append(item)

if missing:
    sys.exit(f"❌  Unrecognised names: {missing}\nRun with --list to see valid country names.")

iso_query    = "".join(iso_params)
region_query = "".join(region_params)

# ---------------------------------------------------------------------------
# 4. Time window & URL
# ---------------------------------------------------------------------------
DAYS_BACK = 14
today      = dt.date.today()
start_date = today - dt.timedelta(days=DAYS_BACK)

url = (
    "https://api.acleddata.com/acled/readme?"
    f"email={EMAIL}&key={TOKEN}"
    f"{iso_query}{region_query}"
    f"&event_date={start_date}|{today}"
    f"&limit=20000&format=json"
)

print("⬇️  Fetching ACLED:", url)
resp = requests.get(url, timeout=60)
resp.raise_for_status()
js = resp.json()
rows = js.get("data", [])
if not rows:
    sys.exit("⚠️  Query succeeded but returned zero rows – check region permissions or spelling.")

df = pd.DataFrame(rows)

# ---------------------------------------------------------------------------
# 5. Save + load to PostGIS
# ---------------------------------------------------------------------------
raw_dir = Path("data/raw")
raw_dir.mkdir(exist_ok=True, parents=True)
out_csv = raw_dir / f"acled_{'_'.join(args)}_{today}.csv"
df.to_csv(out_csv, index=False)
print(f"💾  Saved {len(df):,} rows → {out_csv}")

pg_url = "postgresql://postgres:postgres@db:5432/pathfinder"
create_engine(pg_url).connect().close()  # quick connectivity test
df.to_sql("events_raw", pg_url, if_exists="replace", index=False)
print("✅  Written to PostGIS table events_raw")
