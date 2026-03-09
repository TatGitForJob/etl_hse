CREATE DATABASE airflow;
CREATE DATABASE warehouse;

\connect warehouse

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE raw.user_sessions (
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    session_duration_minutes NUMERIC(10, 2) NOT NULL,
    pages_visited JSONB NOT NULL,
    device JSONB NOT NULL,
    actions JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, start_time)
) PARTITION BY RANGE (start_time);

CREATE TABLE raw.user_sessions_default
    PARTITION OF raw.user_sessions DEFAULT;

CREATE INDEX idx_user_sessions_user_id ON raw.user_sessions (user_id);
CREATE INDEX idx_user_sessions_start_time ON raw.user_sessions (start_time);

CREATE TABLE marts.user_activity_daily (
    activity_date DATE NOT NULL,
    user_id TEXT NOT NULL,
    sessions_count INTEGER NOT NULL,
    total_duration_minutes NUMERIC(12, 2) NOT NULL,
    avg_duration_minutes NUMERIC(12, 2) NOT NULL,
    pages_visited_count INTEGER NOT NULL,
    actions_count INTEGER NOT NULL,
    devices JSONB NOT NULL,
    PRIMARY KEY (activity_date, user_id)
);

CREATE TABLE marts.page_action_popularity_daily (
    activity_date DATE NOT NULL,
    item_type TEXT NOT NULL CHECK (item_type IN ('page', 'action')),
    item_value TEXT NOT NULL,
    hits INTEGER NOT NULL,
    unique_users INTEGER NOT NULL,
    PRIMARY KEY (activity_date, item_type, item_value)
);
