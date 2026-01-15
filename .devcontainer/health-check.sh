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
