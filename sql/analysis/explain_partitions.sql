-- See partition pruning and the index at work.

-- One day: the plan only scans the partition of that day.
EXPLAIN ANALYZE
SELECT count(*) FROM silver.events WHERE event_date = DATE '2026-10-21';

-- One user over two days: an index scan on (event_date, user_id) in the
-- two matching partitions instead of reading every row.
EXPLAIN ANALYZE
SELECT event_type, count(*)
FROM silver.events
WHERE event_date BETWEEN DATE '2026-10-21' AND DATE '2026-10-22'
  AND user_id = 42
GROUP BY event_type;
