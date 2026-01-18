---
layout: default
title: Grafana Setup & Dashboards
nav_order: 9
---

# Grafana Setup & Dashboards

This guide covers how to configure Grafana for monitoring the Football Analytics API, including datasource setup, dashboard creation, and key metrics to track.

## Table of Contents

1. [Access Credentials](#access-credentials)
2. [Adding Datasources](#adding-datasources)
3. [Creating Dashboards](#creating-dashboards)
4. [Dashboard JSON](#dashboard-json)
5. [Key Metrics Reference](#key-metrics-reference)
6. [Querying Metrics](#querying-metrics)

---

## Access Credentials

After deployment, credentials are stored on the server at `/opt/football/credentials.txt`.

### Retrieve Credentials via AWS SSM

```bash
# Get instance ID
INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=*football*" "Name=instance-state-name,Values=running" \
  --query "Reservations[*].Instances[*].InstanceId" --output text)

# Send SSM command to retrieve credentials
COMMAND_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE_ID" \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["cat /opt/football/credentials.txt"]' \
  --query "Command.CommandId" --output text)

# Wait and get output
sleep 3
aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" \
  --instance-id "$INSTANCE_ID" \
  --query "StandardOutputContent" --output text
```

### Service URLs

| Service | URL Pattern | Default Port |
|---------|-------------|--------------|
| Grafana | `https://grafana.<DOMAIN>` | 3000 |
| Prometheus | `https://prometheus.<DOMAIN>` | 9090 |
| API | `https://api.<DOMAIN>` | 8080 |

---

## Adding Datasources

### Option 1: Via Grafana UI

1. Open Grafana: `https://grafana.<your-domain>`
2. Login with admin credentials
3. Go to **Connections** → **Data sources** → **Add data source**
4. Select **Prometheus**
5. Configure:
   - **Name:** `Prometheus`
   - **URL:** `http://prometheus:9090` (internal Docker network)
   - **Access:** `Server (default)`
6. Click **Save & Test**

### Option 2: Via API (Automated)

```bash
# Add Prometheus datasource
curl -X POST -u "admin:<GRAFANA_PASSWORD>" \
  -H "Content-Type: application/json" \
  "https://grafana.<DOMAIN>/api/datasources" \
  -d '{
    "name": "Prometheus",
    "type": "prometheus",
    "url": "http://prometheus:9090",
    "access": "proxy",
    "isDefault": true
  }'
```

### Option 3: Via Provisioning (GitOps)

Create `/infra/grafana/provisioning/datasources/datasources.yml`:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false

  - name: ClickHouse
    type: grafana-clickhouse-datasource
    access: proxy
    url: http://clickhouse:8123
    jsonData:
      defaultDatabase: football_simulator
    secureJsonData:
      password: ${CLICKHOUSE_PASSWORD}
    editable: false
```

---

## Creating Dashboards

### Option 1: Via Grafana UI

1. Click **Dashboards** → **New** → **New Dashboard**
2. Click **Add visualization**
3. Select **Prometheus** datasource
4. Enter PromQL query (see [Key Metrics](#key-metrics-reference))
5. Configure visualization options
6. Click **Apply**
7. **Save dashboard** (Ctrl+S)

### Option 2: Via API (Automated)

```bash
# Create dashboard from JSON file
curl -X POST -u "admin:<GRAFANA_PASSWORD>" \
  -H "Content-Type: application/json" \
  "https://grafana.<DOMAIN>/api/dashboards/db" \
  -d @dashboard.json
```

### Option 3: Import JSON

1. Go to **Dashboards** → **New** → **Import**
2. Paste JSON or upload file
3. Select datasource
4. Click **Import**

---

## Dashboard JSON

### Football API Performance Dashboard

Save this as `infra/grafana/dashboards/football-api-performance.json`:

```json
{
  "dashboard": {
    "title": "Football API Performance",
    "uid": "football-api-perf",
    "tags": ["football", "api", "load-testing"],
    "timezone": "browser",
    "refresh": "5s",
    "time": {
      "from": "now-1h",
      "to": "now"
    },
    "panels": [
      {
        "id": 1,
        "title": "Request Rate (req/s)",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
        "targets": [
          {
            "expr": "sum(rate(http_requests_total{path=~\"/api/.*\"}[1m])) by (path)",
            "legendFormat": "{{path}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "reqps",
            "custom": {
              "drawStyle": "line",
              "lineWidth": 2,
              "fillOpacity": 10
            }
          }
        }
      },
      {
        "id": 2,
        "title": "Response Time (p95)",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
        "targets": [
          {
            "expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{path=~\"/api/.*\"}[1m])) by (le, path))",
            "legendFormat": "{{path}} p95"
          },
          {
            "expr": "histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{path=~\"/api/.*\"}[1m])) by (le, path))",
            "legendFormat": "{{path}} p50"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "thresholds": {
              "steps": [
                {"color": "green", "value": null},
                {"color": "yellow", "value": 0.2},
                {"color": "red", "value": 0.5}
              ]
            }
          }
        }
      },
      {
        "id": 3,
        "title": "Total Events Ingested",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 0, "y": 8},
        "targets": [
          {
            "expr": "sum(events_ingested_total)",
            "legendFormat": "Events"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "short",
            "color": {"mode": "thresholds"},
            "thresholds": {
              "steps": [
                {"color": "blue", "value": null}
              ]
            }
          }
        }
      },
      {
        "id": 4,
        "title": "Total Engagements Ingested",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 6, "y": 8},
        "targets": [
          {
            "expr": "sum(engagements_ingested_total)",
            "legendFormat": "Engagements"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "short",
            "color": {"mode": "thresholds"},
            "thresholds": {
              "steps": [
                {"color": "purple", "value": null}
              ]
            }
          }
        }
      },
      {
        "id": 5,
        "title": "HTTP Errors",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 12, "y": 8},
        "targets": [
          {
            "expr": "sum(http_requests_total{status=~\"4..|5..\"}) or vector(0)",
            "legendFormat": "Errors"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "steps": [
                {"color": "green", "value": null},
                {"color": "yellow", "value": 1},
                {"color": "red", "value": 10}
              ]
            }
          }
        }
      },
      {
        "id": 6,
        "title": "Kafka Errors",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 18, "y": 8},
        "targets": [
          {
            "expr": "sum(kafka_produce_errors_total) or vector(0)",
            "legendFormat": "Kafka Errors"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "steps": [
                {"color": "green", "value": null},
                {"color": "red", "value": 1}
              ]
            }
          }
        }
      },
      {
        "id": 7,
        "title": "Events by Type",
        "type": "piechart",
        "gridPos": {"h": 8, "w": 8, "x": 0, "y": 12},
        "targets": [
          {
            "expr": "sum(events_ingested_total) by (event_type)",
            "legendFormat": "{{event_type}}"
          }
        ],
        "options": {
          "legend": {
            "displayMode": "table",
            "placement": "right",
            "values": ["value", "percent"]
          }
        }
      },
      {
        "id": 8,
        "title": "Engagements by Type",
        "type": "piechart",
        "gridPos": {"h": 8, "w": 8, "x": 8, "y": 12},
        "targets": [
          {
            "expr": "sum(engagements_ingested_total) by (engagement_type)",
            "legendFormat": "{{engagement_type}}"
          }
        ],
        "options": {
          "legend": {
            "displayMode": "table",
            "placement": "right",
            "values": ["value", "percent"]
          }
        }
      },
      {
        "id": 9,
        "title": "Load per Instance",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 8, "x": 16, "y": 12},
        "targets": [
          {
            "expr": "sum(rate(http_requests_total[1m])) by (instance)",
            "legendFormat": "{{instance}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "reqps"
          }
        }
      },
      {
        "id": 10,
        "title": "Engagement Ingest Rate",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 0, "y": 20},
        "targets": [
          {
            "expr": "sum(rate(engagements_ingested_total[1m]))",
            "legendFormat": "Engagements/s"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "short",
            "custom": {
              "fillOpacity": 20
            }
          }
        }
      },
      {
        "id": 11,
        "title": "Events Ingest Rate",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 12, "x": 12, "y": 20},
        "targets": [
          {
            "expr": "sum(rate(events_ingested_total[1m]))",
            "legendFormat": "Events/s"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "short",
            "custom": {
              "fillOpacity": 20
            }
          }
        }
      },
      {
        "id": 12,
        "title": "Avg Response Time by Endpoint",
        "type": "gauge",
        "gridPos": {"h": 6, "w": 24, "x": 0, "y": 28},
        "targets": [
          {
            "expr": "rate(http_request_duration_seconds_sum{path=~\"/api/.*\"}[5m]) / rate(http_request_duration_seconds_count{path=~\"/api/.*\"}[5m])",
            "legendFormat": "{{path}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "unit": "s",
            "min": 0,
            "max": 1,
            "thresholds": {
              "steps": [
                {"color": "green", "value": null},
                {"color": "yellow", "value": 0.2},
                {"color": "red", "value": 0.5}
              ]
            }
          }
        }
      }
    ]
  },
  "overwrite": true
}
```

### Deploy Dashboard via API

```bash
curl -X POST -u "admin:<GRAFANA_PASSWORD>" \
  -H "Content-Type: application/json" \
  "https://grafana.<DOMAIN>/api/dashboards/db" \
  -d @infra/grafana/dashboards/football-api-performance.json
```

---

## Key Metrics Reference

### HTTP Metrics (Go API)

| Metric | Type | Description |
|--------|------|-------------|
| `http_requests_total` | Counter | Total HTTP requests by method, path, status |
| `http_request_duration_seconds` | Histogram | Request duration in seconds |

**Labels:**
- `method`: HTTP method (GET, POST)
- `path`: Request path (/api/events, /api/engagements)
- `status`: HTTP status code (200, 202, 400, 500)
- `instance`: Pod/container IP

### Business Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `events_ingested_total` | Counter | Game events ingested |
| `engagements_ingested_total` | Counter | Engagement events ingested |
| `engagement_ingest_duration_seconds` | Histogram | Time to process engagements |

**Labels:**
- `event_type`: goal, pass, shot, foul, etc.
- `engagement_type`: reaction, comment, share, click, etc.

### Error Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_produce_errors_total` | Counter | Kafka producer errors |
| `clickhouse_query_errors_total` | Counter | ClickHouse query errors |

---

## Querying Metrics

### Using Prometheus API

```bash
# Base URL
PROM_URL="https://prometheus.<DOMAIN>"
AUTH="admin:<PROMETHEUS_PASSWORD>"

# Query total events
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)"

# Query request rate (last 5 minutes)
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=rate(http_requests_total[5m])"

# Query p95 latency
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=histogram_quantile(0.95,rate(http_request_duration_seconds_bucket[5m]))"

# List all available metrics
curl -s -u "$AUTH" "$PROM_URL/api/v1/label/__name__/values" | jq '.data[]'
```

### Common PromQL Queries

#### Request Rate
```promql
# Total request rate
sum(rate(http_requests_total[1m]))

# Request rate by endpoint
sum(rate(http_requests_total{path=~"/api/.*"}[1m])) by (path)

# Request rate by instance (load balancing check)
sum(rate(http_requests_total[1m])) by (instance)
```

#### Latency
```promql
# Average latency
rate(http_request_duration_seconds_sum[5m]) / rate(http_request_duration_seconds_count[5m])

# P50 latency
histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))

# P95 latency
histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))

# P99 latency
histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))
```

#### Error Rate
```promql
# Error rate percentage
sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) * 100

# Errors by status code
sum(http_requests_total{status=~"4..|5.."}) by (status)
```

#### Throughput
```promql
# Events per second
sum(rate(events_ingested_total[1m]))

# Engagements per second
sum(rate(engagements_ingested_total[1m]))

# Events by type
sum(rate(events_ingested_total[1m])) by (event_type)
```

---

## Performance Measurement Workflow

### 1. Before Load Test

```bash
# Note current totals
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)" | jq '.data.result[0].value[1]'
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(engagements_ingested_total)" | jq '.data.result[0].value[1]'
```

### 2. Run Load Test

```bash
cd tests/load
./venv/bin/python match_simulator.py \
  --api-url "https://api.<DOMAIN>" \
  --api-key "<API_KEY>" \
  --viewers 1000 \
  --duration 5
```

### 3. After Load Test - Collect Metrics

```bash
# Total events ingested
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)" | \
  jq -r '"Events: " + .data.result[0].value[1]'

# Total engagements ingested
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(engagements_ingested_total)" | \
  jq -r '"Engagements: " + .data.result[0].value[1]'

# Error counts
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(kafka_produce_errors_total)" | \
  jq -r '"Kafka Errors: " + (.data.result[0].value[1] // "0")'

# Events by type
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)by(event_type)" | \
  jq -r '.data.result[] | "\(.metric.event_type): \(.value[1])"'

# Engagements by type
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(engagements_ingested_total)by(engagement_type)" | \
  jq -r '.data.result[] | "\(.metric.engagement_type): \(.value[1])"'
```

### 4. Generate Performance Report

```bash
#!/bin/bash
# save as: scripts/metrics-report.sh

PROM_URL="${PROMETHEUS_URL:-https://prometheus.football.capibridge.com}"
AUTH="admin:${PROMETHEUS_PASSWORD}"

echo "=============================================="
echo "PERFORMANCE METRICS REPORT"
echo "Generated: $(date)"
echo "=============================================="

echo ""
echo "--- Totals ---"
echo "Events: $(curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)" | jq -r '.data.result[0].value[1] // "0"')"
echo "Engagements: $(curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(engagements_ingested_total)" | jq -r '.data.result[0].value[1] // "0"')"

echo ""
echo "--- Error Counts ---"
echo "Kafka Errors: $(curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(kafka_produce_errors_total)" | jq -r '.data.result[0].value[1] // "0"')"
echo "ClickHouse Errors: $(curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(clickhouse_query_errors_total)" | jq -r '.data.result[0].value[1] // "0"')"

echo ""
echo "--- Events by Type ---"
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(events_ingested_total)by(event_type)" | \
  jq -r '.data.result[] | "  \(.metric.event_type): \(.value[1])"' | sort -t: -k2 -rn

echo ""
echo "--- Engagements by Type ---"
curl -s -u "$AUTH" "$PROM_URL/api/v1/query?query=sum(engagements_ingested_total)by(engagement_type)" | \
  jq -r '.data.result[] | "  \(.metric.engagement_type): \(.value[1])"' | sort -t: -k2 -rn

echo ""
echo "=============================================="
```

---

## Alerting (Optional)

### Create Alert Rule in Grafana

1. Go to **Alerting** → **Alert rules** → **New alert rule**
2. Configure:
   - **Name:** High Error Rate
   - **Query:** `sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01`
   - **Condition:** When query returns value > 0.01 (1% error rate)
3. Set notification channel
4. Save

### Example Alert Rules

```yaml
# High error rate
- alert: HighErrorRate
  expr: sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m])) > 0.01
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "High error rate detected"

# High latency
- alert: HighLatency
  expr: histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le)) > 0.5
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "P95 latency above 500ms"

# Kafka errors
- alert: KafkaErrors
  expr: increase(kafka_produce_errors_total[5m]) > 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "Kafka producer errors detected"
```

---

## Troubleshooting

### Datasource Connection Failed

```bash
# Test Prometheus from within Docker network
docker exec -it $(docker ps -qf name=grafana) \
  wget -qO- http://prometheus:9090/api/v1/status/config
```

### No Metrics Showing

1. Check Prometheus targets: `https://prometheus.<DOMAIN>/targets`
2. Verify services are exposing metrics:
   ```bash
   curl http://localhost:8080/metrics  # from server
   ```
3. Check Prometheus scrape config

### Dashboard Not Loading

1. Verify datasource is set as default
2. Check time range (use "Last 1 hour")
3. Ensure metrics exist in Prometheus
