#!/usr/bin/env python3
from pathlib import Path
import os, itertools, pandas as pd, sqlalchemy as sa

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
DB_URL = os.getenv("DATABASE_URL",
                   "postgresql://postgres:postgres@db:5432/pathfinder")
print("📡  DB_URL =", DB_URL)
engine = sa.create_engine(DB_URL)

# ── locate newest workbook ──────────────────────────────────────────
candidates = list(itertools.chain(
    RAW_DIR.glob("sudan*_pv_*xlsx"),
    RAW_DIR.glob("sudan_hrp_*violence*xlsx"),
))
wb_path = max(candidates, key=lambda p: p.stat().st_mtime)
print("👉  reading", wb_path.relative_to(ROOT))

# ── read the first sheet that has a Month column ────────────────────
for sheet in pd.ExcelFile(wb_path).sheet_names:
    df = pd.read_excel(wb_path, sheet_name=sheet, dtype=str)
    if "Month" in df.columns:
        break
else:
    raise ValueError("No sheet with a 'Month' column found")

# ── tidy dataframe ──────────────────────────────────────────────────
df = (df.rename(columns={
        "Month":"event_month","Year":"event_year",
        "Admin1":"admin1","Admin2":"admin2",
        "Events":"events","Fatalities":"fatalities"})
      [["event_month","event_year","admin1","admin2","events","fatalities"]]
      .assign(month_start=lambda d:
              pd.to_datetime(d.event_month.str[:3]+" "+d.event_year,
                             format="%b %Y"))
      .astype({"events":"Int64","fatalities":"Int64","event_year":"int"}))
print(df.head())

with engine.begin() as conn:
    # 1️⃣ load / replace raw ----------------------------------------
    df.to_sql("acled_monthly_raw", conn,
              if_exists="replace", index=False, method="multi")
    print(f"✅  inserted {len(df):,} rows into acled_monthly_raw")

    # 2️⃣ ensure staging & clean exist ------------------------------
    ddl = Path(ROOT/"sql/02_staging_clean.sql").read_text()
    conn.exec_driver_sql(ddl)
    print("🔑  ensured staging + clean tables exist")

    # 3️⃣ staging ➜ clean upsert ------------------------------------
    UPSERT_SQL = """
    INSERT INTO acled_monthly_staging (...)
    -- (same UPSERT block as before)
    """
    conn.exec_driver_sql(UPSERT_SQL)

print("🎉  staging→clean sync complete")
