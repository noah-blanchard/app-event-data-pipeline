-- Daily active users over the last 7 loaded days.
-- Relative to the latest loaded day, not CURRENT_DATE, so it also works on
-- sample data generated in the past.
SELECT event_date, dau, new_users, returning_users
FROM gold.daily_active_users
WHERE event_date > (SELECT max(event_date) FROM gold.daily_active_users) - 7
ORDER BY event_date;
