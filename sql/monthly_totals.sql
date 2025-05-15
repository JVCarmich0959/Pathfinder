SELECT month_start,
       SUM(events)      AS events,
       SUM(fatalities)  AS fatalities
FROM   acled_monthly_raw
GROUP  BY month_start
ORDER  BY month_start DESC;
