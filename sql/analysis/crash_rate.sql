-- Device models with the highest crash rate on the latest loaded day.
-- A rate on a handful of users is noise: require at least 1,000 of them.
SELECT device_model, active_users, crash_count, crash_rate
FROM gold.device_daily
WHERE event_date = (SELECT max(event_date) FROM gold.device_daily)
  AND active_users >= 1000
ORDER BY crash_rate DESC
LIMIT 10;
