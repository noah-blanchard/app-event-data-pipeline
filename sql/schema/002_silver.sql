-- Valid, deduplicated events (see README, "Data model").
-- Partitioned by event_date: a query on a date range only scans those days,
-- and a day can be replaced without touching the others. The daily
-- partitions are created by the load job, when it sees a new date.
CREATE TABLE IF NOT EXISTS silver.events (
    event_id     UUID        NOT NULL,
    user_id      BIGINT      NOT NULL,
    device_model TEXT        NOT NULL,
    os_version   TEXT,
    app_id       TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    ingested_ts  TIMESTAMPTZ NOT NULL,
    duration_ms  INTEGER,
    country      CHAR(2),
    event_date   DATE        NOT NULL,
    -- On a partitioned table, a unique key must contain the partition key:
    -- each partition only checks its own rows. Uniqueness of event_id alone
    -- is verified by the quality checks.
    PRIMARY KEY (event_id, event_date)
) PARTITION BY RANGE (event_date);

-- Catches rows whose date has no partition yet, instead of failing the insert.
CREATE TABLE IF NOT EXISTS silver.events_default
    PARTITION OF silver.events DEFAULT;

-- User-level queries on a date range. Declared once on the parent table,
-- PostgreSQL creates it on every partition, including future ones.
CREATE INDEX IF NOT EXISTS events_event_date_user_id_idx
    ON silver.events (event_date, user_id);

-- Rejected rows, keyed by the batch they arrived in: a rejected row may have
-- no valid event_date. No primary key: nothing in a bad row is reliable.
CREATE TABLE IF NOT EXISTS silver.rejected_events (
    raw_payload JSONB       NOT NULL,
    reason      TEXT        NOT NULL,
    batch_date  DATE        NOT NULL,
    rejected_ts TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS rejected_events_batch_date_idx
    ON silver.rejected_events (batch_date);
