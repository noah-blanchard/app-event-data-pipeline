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

-- Grain: one row per day and app.
CREATE TABLE IF NOT EXISTS gold.app_usage_daily (
    event_date        DATE             NOT NULL,
    app_id            TEXT             NOT NULL,
    active_users      INTEGER          NOT NULL,
    -- One session = one app_close event, which carries the session length.
    sessions          INTEGER          NOT NULL,
    -- BIGINT: an INTEGER of milliseconds overflows after about 25 days.
    total_duration_ms BIGINT           NOT NULL,
    -- Null when the app has no session that day.
    avg_session_ms    DOUBLE PRECISION,
    PRIMARY KEY (event_date, app_id)
);
