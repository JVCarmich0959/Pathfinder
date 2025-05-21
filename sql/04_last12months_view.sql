-- -----------------------------------------------------------------
-- 04_last12months_view.sql  –  rolling 12 month aggregates
-- Creates a materialized view summarising events and fatalities over
-- the most recent twelve months in acled_monthly_raw.
-- -----------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS postgis;

DROP MATERIALIZED VIEW IF EXISTS acled_last12months_mv;
CREATE MATERIALIZED VIEW acled_last12months_mv AS
SELECT month_start,
       SUM(events)     AS events,
       SUM(fatalities) AS fatalities
FROM   acled_monthly_raw
WHERE  month_start >= (date_trunc('month', NOW()) - INTERVAL '12 months')
GROUP BY month_start
ORDER BY month_start;

CREATE INDEX IF NOT EXISTS acled_last12months_mv_idx
    ON acled_last12months_mv(month_start);
