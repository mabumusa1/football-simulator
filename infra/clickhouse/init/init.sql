-- =============================================================================
-- Fanfinity ClickHouse Schema Initialization
-- =============================================================================
-- This script runs automatically on first container start
-- =============================================================================

-- Create the main database
CREATE DATABASE IF NOT EXISTS fanfinity;

-- =============================================================================
-- API Events Table
-- =============================================================================
-- Stores all API request metrics from the Go application
-- Partitioned by month for efficient data retention management
-- =============================================================================
CREATE TABLE IF NOT EXISTS fanfinity.api_events (
    -- Unique event identifier
    event_id UUID DEFAULT generateUUIDv4(),

    -- Timestamp when the event occurred
    timestamp DateTime DEFAULT now(),

    -- HTTP request details
    endpoint String,
    method LowCardinality(String),      -- GET, POST, PUT, DELETE, etc.
    status_code UInt16,

    -- Performance metrics
    response_time_ms UInt32,
    request_size_bytes UInt32 DEFAULT 0,
    response_size_bytes UInt32 DEFAULT 0,

    -- Client information
    user_agent String DEFAULT '',
    ip_address String DEFAULT '',

    -- Optional: User tracking
    user_id String DEFAULT '',
    session_id String DEFAULT '',

    -- Optional: Additional context
    extra Map(String, String) DEFAULT map()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, endpoint, method)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- =============================================================================
-- Materialized View: Hourly Aggregates
-- =============================================================================
-- Pre-aggregates data for faster dashboard queries
-- =============================================================================
CREATE TABLE IF NOT EXISTS fanfinity.api_events_hourly (
    hour DateTime,
    endpoint String,
    method LowCardinality(String),

    -- Counts
    request_count UInt64,
    error_count UInt64,        -- status_code >= 400

    -- Response time percentiles
    avg_response_time Float64,
    p50_response_time Float64,
    p95_response_time Float64,
    p99_response_time Float64,
    max_response_time UInt32,

    -- Size metrics
    total_request_bytes UInt64,
    total_response_bytes UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, endpoint, method);

CREATE MATERIALIZED VIEW IF NOT EXISTS fanfinity.api_events_hourly_mv
TO fanfinity.api_events_hourly AS
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
FROM fanfinity.api_events
GROUP BY hour, endpoint, method;

-- =============================================================================
-- Useful Queries (for reference)
-- =============================================================================
--
-- Get request rate per endpoint (last hour):
-- SELECT
--     endpoint,
--     count() / 3600 AS requests_per_second
-- FROM fanfinity.api_events
-- WHERE timestamp > now() - INTERVAL 1 HOUR
-- GROUP BY endpoint
-- ORDER BY requests_per_second DESC;
--
-- Get error rate by endpoint:
-- SELECT
--     endpoint,
--     countIf(status_code >= 400) / count() * 100 AS error_rate_percent
-- FROM fanfinity.api_events
-- WHERE timestamp > now() - INTERVAL 1 HOUR
-- GROUP BY endpoint
-- ORDER BY error_rate_percent DESC;
--
-- Get response time percentiles:
-- SELECT
--     endpoint,
--     quantile(0.50)(response_time_ms) AS p50,
--     quantile(0.95)(response_time_ms) AS p95,
--     quantile(0.99)(response_time_ms) AS p99
-- FROM fanfinity.api_events
-- WHERE timestamp > now() - INTERVAL 1 HOUR
-- GROUP BY endpoint;
-- =============================================================================
