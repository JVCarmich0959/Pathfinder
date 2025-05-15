#!/usr/bin/env python3
"""
HDX Sudan workbook  ➜  acled_monthly_raw ➜ staging ➜ clean
Works from any CWD, any container, any CI.
"""

from pathlib import Path
import os, itertools, pandas as pd
from sqlalchemy import create_engine

# ── paths & DB url ──────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
RAW  = ROOT / "data" / "raw"

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/pathfinder",   # default for docker-compose
)
print("📡  DB_URL =", DB_URL)        # will show in CI log
engine = create_engine(DB_URL)

# ── locate newest workbook ─────────────────────────────────────
candidates = list(itertools.chain(
    RAW.glob("sudan*_pv_*xlsx"),
    RAW.glob("sudan_hrp_*violence*xlsx"),
))
if not candidates:
    raise FileNotFoundError(f"No workbook in {RAW}")

wb_path = max(candidates, key=lambda p: p.stat().st_mtime)
print("👉  reading", wb_path.relative_to(ROOT))

# ── read first sheet that has a Month column ───────────────────
for sheet in pd.ExcelFile(wb_path).sheet_names:
    df = pd.read_excel(wb_path, sheet_name=sheet, dtype=str)
    if "Month" in df.columns:
        break
else:
    raise ValueError("No sheet with a 'Month' column found")

# ── tidy data ──────────────────────────────────────────────────
df = (df.rename(columns={
        "Month": "event_month", "Year": "event_year",
        "Admin1": "admin1",     "Admin2": "admin2",
        "Events": "events",     "Fatalities": "fatalities",
    })[["event_month","event_year","admin1","admin2","events","fatalities"]]
    .assign(month_start=lambda d:
            pd.to_datetime(d.event_month.str[:3] + " " + d.event_year,
                           format="%b %Y"))
    .astype({"events":"Int64","fatalities":"Int64","event_year":"int"})
)
print(df.head())

# ── load raw table ─────────────────────────────────────────────
df.to_sql("acled_monthly_raw", engine, if_exists="replace",
          index=False, method="multi")
print(f"✅  inserted {len(df):,} rows into acled_monthly_raw")

# ── staging ➜ clean upsert ─────────────────────────────────────
UPSERT_SQL = """
INSERT INTO acled_monthly_staging (event_month,event_year,admin1,admin2,
                                   events,fatalities,month_start)
SELECT event_month,event_year,admin1,admin2,events,fatalities,month_start
FROM   acled_monthly_raw
ON CONFLICT DO NOTHING;

UPDATE acled_monthly_staging s SET
  _row_hash = md5(concat_ws('‖',s.event_month,s.event_year::text,
                            s.admin2,coalesce(s.fatalities,0)::text))
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
with engine.begin() as conn:
    conn.exec_driver_sql(UPSERT_SQL)

print("🎉  staging→clean sync complete")
