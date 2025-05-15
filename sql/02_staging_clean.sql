-- ------------------------------------------------------------------
-- 02_staging_clean.sql  –  staging → clean (no natural PK available)
-- ------------------------------------------------------------------

-- 1. STAGING  (mirror raw + bookkeeping)
CREATE TABLE acled_monthly_staging
  (LIKE acled_monthly_raw INCLUDING INDEXES INCLUDING DEFAULTS);

ALTER TABLE acled_monthly_staging
  ADD COLUMN _row_hash  text,
  ADD COLUMN _loaded_at timestamptz DEFAULT now();

CREATE INDEX acled_monthly_staging_hash_idx ON acled_monthly_staging(_row_hash);

-- 2. CLEAN  (same cols + surrogate primary key)
CREATE TABLE acled_monthly_clean (
    id serial PRIMARY KEY,                    -- synthetic key
    LIKE acled_monthly_raw INCLUDING INDEXES INCLUDING DEFAULTS,
    _loaded_at timestamptz
);

CREATE INDEX acled_monthly_clean_month_idx ON acled_monthly_clean(month_start);
