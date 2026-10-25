-- Business aggregates, one table per grain (see README, "Data model").
-- Small tables: no partitioning, the primary key on the grain is the index.

-- Grain: one row per day.
CREATE TABLE IF NOT EXISTS gold.daily_active_users (
    event_date      DATE    PRIMARY KEY,
    -- Distinct users with at least one event that day.
    dau             INTEGER NOT NULL,
    -- Users whose first event ever is on that day.
    new_users       INTEGER NOT NULL,
    returning_users INTEGER NOT NULL
);
