#!/usr/bin/env python3
"""
Pull last 14 days of conflict events for Sudan and Chad from ACLED,
store as CSV and push into PostGIS table `events_raw`.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

# --- config ---------------------------------------------------------------

TOKEN = os.getenv("ACLED_TOKEN")
COUNTRIES = "Sudan,Chad"
DAYS_BACK = 14
OUT_CSV = "data/raw/acled_latest.csv"

PG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "db": os.getenv("POSTGRES_DB", "pathfinder"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "pw": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

# --- fetch ACLED ----------------------------------------------------------

today = datetime.utcnow().date()
start_date = today - timedelta(days=DAYS_BACK)
url = (
    "https://api.acleddata.com/acled/readme?email=example@example.com"
    f"&key={TOKEN}&country={COUNTRIES}"
    f"&event_date={start_date}|{today}&limit=0&format=json"
)

print("Fetching ACLED:", url)
resp = requests.get(url, timeout=60)
resp.raise_for_status()
df = pd.DataFrame(resp.json().get("data", []))
if df.empty:
    print("No data returned.")
    exit()

# --- save csv -------------------------------------------------------------

os.makedirs("data/raw", exist_ok=True)
df.to_csv(OUT_CSV, index=False)
print("Saved", OUT_CSV, len(df), "rows")

# --- push to PostGIS ------------------------------------------------------

pg_url = f"postgresql://{PG['user']}:{PG['pw']}@{PG['host']}:5432/{PG['db']}"
engine = create_engine(pg_url)

df.to_sql("events_raw", engine, if_exists="replace", index=False)
print("Written to PostGIS table events_raw")

