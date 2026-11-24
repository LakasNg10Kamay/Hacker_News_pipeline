-- Runs once, the first time the Postgres container starts.
CREATE DATABASE hn;
\connect hn

-- Bronze layer: raw API responses, one row per story per snapshot (UTC)
CREATE TABLE IF NOT EXISTS raw_items (
    item_id      BIGINT      NOT NULL,
    snapshot_ts  TIMESTAMP   NOT NULL,
    rank         INT         NOT NULL,
    payload      JSONB       NOT NULL,
    loaded_at    TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    PRIMARY KEY (item_id, snapshot_ts)
);
