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
