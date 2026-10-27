-- Top 5 apps by total usage time on the latest loaded day.
-- 3600000.0, not 3600000: dividing two integers would truncate the hours.
SELECT app_id,
       total_duration_ms / 3600000.0 AS hours,
       sessions,
       round(avg_session_ms) AS avg_session_ms
FROM gold.app_usage_daily
WHERE event_date = (SELECT max(event_date) FROM gold.app_usage_daily)
ORDER BY total_duration_ms DESC
LIMIT 5;
