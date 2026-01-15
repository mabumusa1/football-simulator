#!/bin/bash
set -e

echo "Running health checks..."

# Wait for services to be ready
echo "Waiting for services to initialize..."
sleep 10

# Check ClickHouse
echo -n "Checking ClickHouse... "
if curl -s --fail http://clickhouse:8123/ping > /dev/null; then
    echo "✓ Ready"

    # Create engagement tables if they don't exist
    echo "Creating ClickHouse engagement tables..."

    curl -s http://clickhouse:8123 -d "
    CREATE TABLE IF NOT EXISTS football_simulator.engagement_events (
        event_id UUID DEFAULT generateUUIDv4(),
        match_id String,
        user_id String,
        session_id String,
        engagement_type LowCardinality(String),
        engagement_subtype LowCardinality(String),
        related_game_event_id Nullable(UUID),
        game_minute UInt8 DEFAULT 0,
        device_type LowCardinality(String) DEFAULT 'unknown',
        platform LowCardinality(String) DEFAULT 'unknown',
        country_code LowCardinality(String) DEFAULT '',
        content String DEFAULT '',
        metadata String DEFAULT '{}',
        timestamp DateTime64(3) DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY (match_id, engagement_type, timestamp, user_id)
    TTL toDateTime(timestamp) + INTERVAL 30 DAY
    " > /dev/null 2>&1 && echo "  engagement_events ✓" || echo "  engagement_events ⚠"

    curl -s http://clickhouse:8123 -d "
    CREATE TABLE IF NOT EXISTS football_simulator.match_events (
        event_id UUID DEFAULT generateUUIDv4(),
        match_id String,
        event_type LowCardinality(String),
        team_id UInt8,
        player_id String DEFAULT '',
        game_minute UInt8,
        metadata String DEFAULT '{}',
        timestamp DateTime64(3) DEFAULT now64(3)
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMMDD(timestamp)
    ORDER BY (match_id, timestamp, event_type)
    TTL toDateTime(timestamp) + INTERVAL 90 DAY
    " > /dev/null 2>&1 && echo "  match_events ✓" || echo "  match_events ⚠"

else
    echo "✗ Not responding"
    exit 1
fi

# Check Prometheus
echo -n "Checking Prometheus... "
if curl -s --fail http://prometheus:9090/-/healthy > /dev/null; then
    echo "✓ Ready"
else
    echo "✗ Not responding"
    exit 1
fi

# Check Kafka (via Kafka UI)
echo -n "Checking Kafka UI... "
if curl -s --fail http://kafka-ui:8080/actuator/health > /dev/null 2>&1; then
    echo "✓ Ready"

    # Create required Kafka topics
    echo "Creating Kafka topics..."

    # Create events topic (game events on the field)
    echo -n "  Creating football_simulator.events... "
    if curl -s -X POST "http://kafka-ui:8080/api/clusters/football-simulator-local/topics" \
        -H "Content-Type: application/json" \
        -d '{"name":"football_simulator.events","partitions":3,"replicationFactor":1}' > /dev/null 2>&1; then
        echo "✓"
    else
        echo "⚠ (may already exist)"
    fi

    # Create engagements topic (viewer engagement events)
    echo -n "  Creating football_simulator.engagements... "
    if curl -s -X POST "http://kafka-ui:8080/api/clusters/football-simulator-local/topics" \
        -H "Content-Type: application/json" \
        -d '{"name":"football_simulator.engagements","partitions":6,"replicationFactor":1}' > /dev/null 2>&1; then
        echo "✓"
    else
        echo "⚠ (may already exist)"
    fi

    # Create retry topic (failed events for retry)
    echo -n "  Creating football_simulator.retry... "
    if curl -s -X POST "http://kafka-ui:8080/api/clusters/football-simulator-local/topics" \
        -H "Content-Type: application/json" \
        -d '{"name":"football_simulator.retry","partitions":3,"replicationFactor":1}' > /dev/null 2>&1; then
        echo "✓"
    else
        echo "⚠ (may already exist)"
    fi

    # Create dead letter topic
    echo -n "  Creating football_simulator.dead... "
    if curl -s -X POST "http://kafka-ui:8080/api/clusters/football-simulator-local/topics" \
        -H "Content-Type: application/json" \
        -d '{"name":"football_simulator.dead","partitions":1,"replicationFactor":1}' > /dev/null 2>&1; then
        echo "✓"
    else
        echo "⚠ (may already exist)"
    fi
else
    echo "⚠ Kafka UI not responding (may still be starting)"
fi

# Check Grafana
echo -n "Checking Grafana... "
if curl -s --fail http://grafana:3000/api/health > /dev/null 2>&1; then
    echo "✓ Ready"
else
    echo "⚠ Grafana not responding (may still be starting)"
fi

echo ""
echo "=== Service URLs ==="
echo "Kafka UI:    http://localhost:8081"
echo "Grafana:     http://localhost:3005"
echo "Prometheus:  http://localhost:9090"
echo "ClickHouse:  http://localhost:8123"
echo ""
echo "All critical services are ready!"
