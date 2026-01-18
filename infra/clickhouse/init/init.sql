-- Initialize football_simulator database and schema
CREATE DATABASE IF NOT EXISTS football_simulator;

CREATE TABLE IF NOT EXISTS football_simulator.api_events (
    event_id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime DEFAULT now(),
    endpoint String,
    method LowCardinality(String),
    status_code UInt16,
    response_time_ms UInt32,
    request_size_bytes UInt32 DEFAULT 0,
    response_size_bytes UInt32 DEFAULT 0,
    user_agent String DEFAULT '',
    ip_address String DEFAULT '',
    user_id String DEFAULT '',
    session_id String DEFAULT '',
    extra Map(String, String) DEFAULT map()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, endpoint, method)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS football_simulator.api_events_hourly (
    hour DateTime,
    endpoint String,
    method LowCardinality(String),
    request_count UInt64,
    error_count UInt64,
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time UInt32,
    total_request_bytes UInt64,
    total_response_bytes UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, endpoint, method);

CREATE MATERIALIZED VIEW IF NOT EXISTS football_simulator.api_events_hourly_mv
TO football_simulator.api_events_hourly AS
SELECT
    toStartOfHour(timestamp) AS hour,
    endpoint,
    method,
    count() AS request_count,
    countIf(status_code >= 400) AS error_count,
    avg(response_time_ms) AS avg_response_time,
    quantile(0.50)(response_time_ms) AS p50_response_time,
    quantile(0.95)(response_time_ms) AS p95_response_time,
    quantile(0.99)(response_time_ms) AS p99_response_time,
    max(response_time_ms) AS max_response_time,
    sum(request_size_bytes) AS total_request_bytes,
    sum(response_size_bytes) AS total_response_bytes
FROM football_simulator.api_events
GROUP BY hour, endpoint, method;

-- =============================================================================
-- Dashboard Views
-- =============================================================================

-- Request rate per endpoint (last hour)
CREATE VIEW IF NOT EXISTS football_simulator.v_request_rate AS
SELECT
    endpoint,
    count() AS total_requests,
    count() / 3600 AS requests_per_second
FROM football_simulator.api_events
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY endpoint
ORDER BY requests_per_second DESC;

-- Error rate by endpoint (last hour)
CREATE VIEW IF NOT EXISTS football_simulator.v_error_rate AS
SELECT
    endpoint,
    count() AS total_requests,
    countIf(status_code >= 400) AS error_count,
    countIf(status_code >= 400) / count() * 100 AS error_rate_percent
FROM football_simulator.api_events
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY endpoint
ORDER BY error_rate_percent DESC;

-- Response time percentiles by endpoint (last hour)
CREATE VIEW IF NOT EXISTS football_simulator.v_response_times AS
SELECT
    endpoint,
    count() AS total_requests,
    avg(response_time_ms) AS avg_ms,
    quantile(0.50)(response_time_ms) AS p50_ms,
    quantile(0.95)(response_time_ms) AS p95_ms,
    quantile(0.99)(response_time_ms) AS p99_ms,
    max(response_time_ms) AS max_ms
FROM football_simulator.api_events
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY endpoint
ORDER BY avg_ms DESC;

-- Overall API health summary (last hour)
CREATE VIEW IF NOT EXISTS football_simulator.v_api_health AS
SELECT
    count() AS total_requests,
    countIf(status_code >= 200 AND status_code < 300) AS success_2xx,
    countIf(status_code >= 300 AND status_code < 400) AS redirect_3xx,
    countIf(status_code >= 400 AND status_code < 500) AS client_error_4xx,
    countIf(status_code >= 500) AS server_error_5xx,
    countIf(status_code >= 400) / count() * 100 AS error_rate_percent,
    avg(response_time_ms) AS avg_response_ms,
    quantile(0.95)(response_time_ms) AS p95_response_ms
FROM football_simulator.api_events
WHERE timestamp > now() - INTERVAL 1 HOUR;

-- Top slow endpoints (last hour)
CREATE VIEW IF NOT EXISTS football_simulator.v_slow_endpoints AS
SELECT
    endpoint,
    method,
    count() AS request_count,
    avg(response_time_ms) AS avg_ms,
    max(response_time_ms) AS max_ms
FROM football_simulator.api_events
WHERE timestamp > now() - INTERVAL 1 HOUR
GROUP BY endpoint, method
ORDER BY avg_ms DESC
LIMIT 10;

CREATE TABLE IF NOT EXISTS football_simulator.engagement_events (
    event_id UUID DEFAULT generateUUIDv4(),
    match_id String,
    user_id String,
    session_id String,

    -- Engagement classification
    engagement_type LowCardinality(String),      -- reaction, comment, video_action, share, prediction, click, session
    engagement_subtype LowCardinality(String),   -- specific action within type

    -- Correlation with game events
    related_game_event_id Nullable(UUID),
    game_minute UInt8 DEFAULT 0,

    -- Context
    device_type LowCardinality(String) DEFAULT 'unknown',  -- mobile, desktop, tablet, tv, unknown
    platform LowCardinality(String) DEFAULT 'unknown',     -- ios, android, web, smart_tv
    country_code LowCardinality(String) DEFAULT '',

    -- Engagement data
    content String DEFAULT '',                   -- comment text, reaction emoji, etc.
    metadata String DEFAULT '{}',                -- JSON blob for additional data

    -- Timestamps
    timestamp DateTime64(3) DEFAULT now64(3),

    -- For deduplication
    INDEX idx_user_session (user_id, session_id) TYPE bloom_filter GRANULARITY 4,
    INDEX idx_game_event (related_game_event_id) TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (match_id, engagement_type, timestamp, user_id)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- Active Sessions Table (for concurrent viewer tracking)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS football_simulator.active_sessions (
    match_id String,
    user_id String,
    session_id String,
    device_type LowCardinality(String),
    platform LowCardinality(String),
    country_code LowCardinality(String) DEFAULT '',
    joined_at DateTime64(3),
    last_heartbeat DateTime64(3),
    left_at Nullable(DateTime64(3)),
    total_watch_time_seconds UInt32 DEFAULT 0,
    engagement_count UInt32 DEFAULT 0
)
ENGINE = ReplacingMergeTree(last_heartbeat)
PARTITION BY toYYYYMMDD(joined_at)
ORDER BY (match_id, user_id, session_id)
TTL toDateTime(joined_at) + INTERVAL 7 DAY;

-- -----------------------------------------------------------------------------
-- Match Events Table (game events that trigger engagement)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS football_simulator.match_events (
    event_id UUID DEFAULT generateUUIDv4(),
    match_id String,
    event_type LowCardinality(String),
    team_id UInt8,
    player_id String DEFAULT '',
    game_minute UInt8,
    metadata String DEFAULT '{}',
    timestamp DateTime64(3) DEFAULT now64(3),

    INDEX idx_match_minute (match_id, game_minute) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (match_id, timestamp, event_type)
TTL toDateTime(timestamp) + INTERVAL 90 DAY;

-- =============================================================================
-- Materialized Views for Real-time Analytics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Engagement per minute (for engagement timeline)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS football_simulator.engagement_per_minute (
    match_id String,
    game_minute UInt8,
    engagement_type LowCardinality(String),
    engagement_count SimpleAggregateFunction(sum, UInt64),
    unique_users AggregateFunction(uniq, String),
    window_start DateTime
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (match_id, game_minute, engagement_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS football_simulator.engagement_per_minute_mv
TO football_simulator.engagement_per_minute AS
SELECT
    match_id,
    game_minute,
    engagement_type,
    count() AS engagement_count,
    uniqState(user_id) AS unique_users,
    toStartOfMinute(timestamp) AS window_start
FROM football_simulator.engagement_events
GROUP BY match_id, game_minute, engagement_type, window_start;

-- -----------------------------------------------------------------------------
-- Engagement by type aggregation
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS football_simulator.engagement_by_type (
    match_id String,
    engagement_type LowCardinality(String),
    engagement_subtype LowCardinality(String),
    total_count SimpleAggregateFunction(sum, UInt64),
    unique_users AggregateFunction(uniq, String),
    hour DateTime
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (match_id, engagement_type, engagement_subtype, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS football_simulator.engagement_by_type_mv
TO football_simulator.engagement_by_type AS
SELECT
    match_id,
    engagement_type,
    engagement_subtype,
    count() AS total_count,
    uniqState(user_id) AS unique_users,
    toStartOfHour(timestamp) AS hour
FROM football_simulator.engagement_events
GROUP BY match_id, engagement_type, engagement_subtype, hour;

-- -----------------------------------------------------------------------------
-- Device/Platform breakdown
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS football_simulator.engagement_by_device (
    match_id String,
    device_type LowCardinality(String),
    platform LowCardinality(String),
    engagement_count SimpleAggregateFunction(sum, UInt64),
    unique_users AggregateFunction(uniq, String),
    hour DateTime
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(hour)
ORDER BY (match_id, device_type, platform, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS football_simulator.engagement_by_device_mv
TO football_simulator.engagement_by_device AS
SELECT
    match_id,
    device_type,
    platform,
    count() AS engagement_count,
    uniqState(user_id) AS unique_users,
    toStartOfHour(timestamp) AS hour
FROM football_simulator.engagement_events
GROUP BY match_id, device_type, platform, hour;

-- =============================================================================
-- Analytics Views
-- =============================================================================

-- Real-time concurrent viewers (last 5 minutes of heartbeats)
CREATE VIEW IF NOT EXISTS football_simulator.v_concurrent_viewers AS
SELECT
    match_id,
    count(DISTINCT user_id) AS concurrent_viewers,
    countIf(device_type = 'mobile') AS mobile_viewers,
    countIf(device_type = 'desktop') AS desktop_viewers,
    countIf(device_type = 'tablet') AS tablet_viewers,
    countIf(device_type = 'tv') AS tv_viewers
FROM football_simulator.active_sessions
WHERE last_heartbeat > now() - INTERVAL 5 MINUTE
  AND left_at IS NULL
GROUP BY match_id;

-- Engagement rate per match (engagements per viewer per minute)
CREATE VIEW IF NOT EXISTS football_simulator.v_engagement_rate AS
SELECT
    e.match_id,
    count() AS total_engagements,
    uniq(e.user_id) AS unique_engaged_users,
    count() / uniq(e.user_id) AS engagements_per_user,
    count() / 90.0 AS engagements_per_minute
FROM football_simulator.engagement_events e
WHERE e.timestamp > now() - INTERVAL 2 HOUR
GROUP BY e.match_id;

-- Peak engagement moments (find spikes)
CREATE VIEW IF NOT EXISTS football_simulator.v_peak_engagement AS
SELECT
    match_id,
    game_minute,
    sum(engagement_count) AS total_engagements,
    uniqMerge(unique_users) AS unique_users
FROM football_simulator.engagement_per_minute
GROUP BY match_id, game_minute
ORDER BY match_id, total_engagements DESC;

-- Engagement type distribution
CREATE VIEW IF NOT EXISTS football_simulator.v_engagement_distribution AS
SELECT
    match_id,
    engagement_type,
    sum(total_count) AS count,
    sum(total_count) * 100.0 / sum(sum(total_count)) OVER (PARTITION BY match_id) AS percentage
FROM football_simulator.engagement_by_type
GROUP BY match_id, engagement_type
ORDER BY match_id, count DESC;

-- Reaction breakdown (most used reactions)
CREATE VIEW IF NOT EXISTS football_simulator.v_reaction_breakdown AS
SELECT
    match_id,
    engagement_subtype AS reaction_type,
    sum(total_count) AS count
FROM football_simulator.engagement_by_type
WHERE engagement_type = 'reaction'
GROUP BY match_id, engagement_subtype
ORDER BY match_id, count DESC;

-- Game event impact (which game events drive most engagement)
CREATE VIEW IF NOT EXISTS football_simulator.v_game_event_impact AS
SELECT
    me.match_id,
    me.event_type AS game_event_type,
    me.game_minute,
    count(ee.event_id) AS engagement_triggered,
    uniq(ee.user_id) AS users_engaged
FROM football_simulator.match_events me
LEFT JOIN football_simulator.engagement_events ee
    ON me.match_id = ee.match_id
    AND ee.game_minute BETWEEN me.game_minute AND me.game_minute + 1
    AND ee.timestamp > me.timestamp
    AND ee.timestamp < me.timestamp + INTERVAL 2 MINUTE
GROUP BY me.match_id, me.event_type, me.game_minute
ORDER BY engagement_triggered DESC;

-- Viewer retention curve
CREATE VIEW IF NOT EXISTS football_simulator.v_viewer_retention AS
SELECT
    match_id,
    game_minute,
    uniq(user_id) AS active_users,
    first_value(uniq(user_id)) OVER (PARTITION BY match_id ORDER BY game_minute) AS initial_users,
    uniq(user_id) * 100.0 / first_value(uniq(user_id)) OVER (PARTITION BY match_id ORDER BY game_minute) AS retention_pct
FROM football_simulator.engagement_events
GROUP BY match_id, game_minute
ORDER BY match_id, game_minute;

