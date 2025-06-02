#!/usr/bin/env python3
"""
load_pv_monthly.py  –  HDX Sudan workbook  ➜  raw ➜ staging ➜ clean
Self-contained: creates missing tables every run, works in Docker or CI.
"""

from pathlib import Path
import itertools
import argparse
import pandas as pd
from pathfinder.settings import DATA_RAW, engine

# ────────────────────────────────────────────────────────────────
# 0.  Paths & database URL
# ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = DATA_RAW

print("📡  DB_URL =", engine().url)

# ------------------------------------------------------------------ CLI
parser = argparse.ArgumentParser(description="Load HDX monthly workbook")
parser.add_argument("--src", help="Path to CSV/XLSX file", default=None)
args = parser.parse_args()

# ────────────────────────────────────────────────────────────────
# 1.  Locate newest workbook
# ────────────────────────────────────────────────────────────────
if args.src:
    wb_path = Path(args.src)
    if not wb_path.exists():
        raise FileNotFoundError(f"File not found: {wb_path}")
else:
    candidates = sorted(DATA_RAW.glob("*monthly*"))
    if not candidates:
        raise FileNotFoundError(
            f"No monthly HDX file found in {DATA_RAW}."
            " Run scripts/fetch_hdx_pv.sh first."
        )
    wb_path = candidates[-1]
print("👉  reading", wb_path.relative_to(ROOT))

# ────────────────────────────────────────────────────────────────
# 2.  Read the sheet that contains a 'Month' column
# ────────────────────────────────────────────────────────────────
if wb_path.suffix.lower().endswith("csv"):
    df = pd.read_csv(wb_path, dtype=str)
else:
    for sheet in pd.ExcelFile(wb_path).sheet_names:
        df = pd.read_excel(wb_path, sheet_name=sheet, dtype=str)
        if "Month" in df.columns:
            break
    else:
        raise ValueError("No sheet with a 'Month' column found!")

# ────────────────────────────────────────────────────────────────
# 3.  Tidy dataframe
# ────────────────────────────────────────────────────────────────
df = (df.rename(columns={
        "Month": "event_month", "Year": "event_year",
        "Admin1": "admin1",     "Admin2": "admin2",
        "Events": "events",     "Fatalities": "fatalities",
    })[["event_month","event_year","admin1","admin2","events","fatalities"]]
      .assign(month_start=lambda d:
              pd.to_datetime(d.event_month.str[:3] + " " + d.event_year,
                             format="%b %Y"))
      .astype({"events":"Int64","fatalities":"Int64","event_year":"int"}))
print(df.head())

# ────────────────────────────────────────────────────────────────
# 4.  Insert / replace raw table  (must come *before* DDL below)
# ────────────────────────────────────────────────────────────────
df.to_sql("acled_monthly_raw", engine(),
          if_exists="replace", index=False, method="multi")
print(f"✅  inserted {len(df):,} rows into acled_monthly_raw")

# ────────────────────────────────────────────────────────────────
# 5.  Ensure staging & clean tables exist (DDL runs after raw exists)
# ────────────────────────────────────────────────────────────────
ddl_sql = (ROOT / "sql" / "02_staging_clean.sql").read_text()
with engine().begin() as conn:
    conn.exec_driver_sql(ddl_sql)
print("🔑  ensured staging & clean tables exist")

# ────────────────────────────────────────────────────────────────
# 6.  staging ➜ clean upsert
# ────────────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO acled_monthly_staging (event_month,event_year,admin1,admin2,
                                   events,fatalities,month_start)
SELECT event_month,event_year,admin1,admin2,events,fatalities,month_start
FROM   acled_monthly_raw
ON CONFLICT DO NOTHING;

UPDATE acled_monthly_staging s SET
  _row_hash = md5(concat_ws('‖',s.event_month,
                                 s.event_year::text,
                                 s.admin2,
                                 coalesce(s.fatalities,0)::text))
WHERE _row_hash IS NULL;

INSERT INTO acled_monthly_clean (event_month,event_year,admin1,admin2,
                                 events,fatalities,month_start,_loaded_at)
SELECT DISTINCT ON (_row_hash)
       event_month,event_year,admin1,admin2,
       events,fatalities,month_start,_loaded_at
FROM   acled_monthly_staging
ORDER  BY _row_hash,_loaded_at DESC
ON CONFLICT DO NOTHING;

DELETE FROM acled_monthly_staging
WHERE _loaded_at < NOW() - INTERVAL '30 days';
"""
with engine().begin() as conn:
    conn.exec_driver_sql(UPSERT_SQL)

print("🎉  staging→clean sync complete")
