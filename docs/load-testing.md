---
layout: default
title: Load Testing
nav_order: 7
---

# Load Testing

Test Fanfinity Infrastructure with realistic football match simulations.

## Overview

The load testing suite simulates real football matches with:
- **100K+ concurrent viewers**
- **Realistic engagement patterns** (reactions spike on goals)
- **10K+ events per minute**
- **Real team rosters** (Al-Hilal vs Al-Nassr)

## Prerequisites

```bash
cd tests/load

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Quick Start

```bash
# Quick test (1,000 viewers, 5 minutes)
python match_simulator.py \
  --api-url http://localhost:8080 \
  --api-key your-api-key \
  --viewers 1000 \
  --duration 5

# Full test (100,000 viewers, 90 minutes)
python match_simulator.py \
  --api-url http://localhost:8080 \
  --api-key your-api-key \
  --viewers 100000 \
  --duration 90
```

## Available Simulators

### 1. Combined Match Simulator

**File:** `match_simulator.py`

Simulates a complete football match with game events and viewer engagement.

```bash
python match_simulator.py \
  --api-url http://localhost:8080 \
  --api-key your-api-key \
  --match-id "match_$(date +%Y%m%d_%H%M%S)" \
  --viewers 100000 \
  --duration 90 \
  --batch-size 500 \
  --concurrency 50
```

**Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--api-url` | http://localhost:8080 | API endpoint |
| `--api-key` | (required) | API authentication key |
| `--match-id` | auto-generated | Unique match identifier |
| `--viewers` | 100000 | Number of concurrent viewers |
| `--duration` | 90 | Match duration in minutes |
| `--batch-size` | 500 | Events per batch request |
| `--concurrency` | 50 | Concurrent HTTP connections |

### 2. Game Events Only

**File:** `simulate_match.py`

Simulates only game events (goals, passes, fouls, etc.).

```bash
python simulate_match.py \
  --api-url http://localhost:8080 \
  --api-key your-api-key \
  --match-id "match_001"
```

### 3. Engagement Events Only

**File:** `viewer_simulator.py`

Simulates viewer engagement without game events.

```bash
python viewer_simulator.py \
  --api-url http://localhost:8080 \
  --api-key your-api-key \
  --match-id "match_001" \
  --viewers 100000 \
  --duration 90
```

## Engagement Correlation

The simulator realistically correlates viewer engagement with game events:

| Game Event | Engagement Multiplier |
|------------|----------------------|
| Goal | 15x baseline |
| Red Card | 10x baseline |
| Penalty | 8x baseline |
| VAR Review | 6x baseline |
| Yellow Card | 4x baseline |
| Shot on Target | 3x baseline |
| Corner | 2x baseline |
| Normal play | 1x baseline |

**Example:** During a goal, if baseline is 100 engagements/second, expect ~1,500 engagements/second.

## Viewer Personas

The simulator models different viewer types:

| Persona | % of Viewers | Characteristics |
|---------|--------------|-----------------|
| Casual | 40% | Low engagement, mobile, short sessions |
| Regular | 35% | Moderate engagement, reactions + comments |
| Superfan | 15% | High engagement, all features, long sessions |
| Analyst | 10% | Stats-focused, desktop, predictions |

## Team Data

The simulator uses real team rosters:

**Al-Hilal** (`alhilal.csv`):
- Full squad with positions
- Player numbers
- Realistic substitution patterns

**Al-Nassr** (`alnassr.csv`):
- Full squad with positions
- Player numbers
- Realistic substitution patterns

## Output Example

```
=== Match Simulator ===
API URL: http://localhost:8080
Match ID: match_20240115_143000
Viewers: 100,000
Duration: 90 minutes

Starting simulation...

[00:00] Match started
[15:22] ⚽ GOAL! Player #10 scores! (Engagement spike: 15,234 events)
[23:45] 🟨 Yellow card - Player #7
[45:00] Half-time (45,230 total events, 98.5% success rate)
[67:12] ⚽ GOAL! Player #9 scores! (Engagement spike: 14,892 events)
[78:30] 🔄 Substitution
[90:00] Full-time

=== Summary ===
Total game events: 1,247
Total engagement events: 892,451
Success rate: 99.2%
Average latency: 23ms
P95 latency: 87ms
P99 latency: 142ms
```

## Monitoring During Tests

### Grafana Dashboard

1. Open http://localhost:3005
2. Login (admin/admin)
3. Navigate to: Dashboards → k6 Load Test
4. Watch real-time metrics

### Prometheus Queries

```promql
# Request rate
sum(rate(http_requests_total[1m]))

# Event ingestion rate
sum(rate(events_ingested_total[1m])) by (type)

# Latency percentiles
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[1m]))
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[1m]))

# Kafka producer errors
sum(rate(kafka_produce_errors_total[1m]))
```

### ClickHouse Queries

```sql
-- Events per minute
SELECT
    toStartOfMinute(timestamp) as minute,
    count() as events
FROM match_events
WHERE match_id = 'match_001'
GROUP BY minute
ORDER BY minute;

-- Engagement by type
SELECT
    engagement_type,
    count() as total
FROM engagement_events
WHERE match_id = 'match_001'
GROUP BY engagement_type
ORDER BY total DESC;

-- Peak engagement minute
SELECT * FROM v_peak_engagement
WHERE match_id = 'match_001';
```

## k6 Load Tests

For more advanced load testing, use k6:

### Run k6 Test

```bash
# Using the script
./scripts/run-k6-test.sh

# Start and follow logs
./scripts/run-k6-test.sh --watch

# Check status
./scripts/run-k6-test.sh --status

# Stop test
./scripts/run-k6-test.sh --stop
```

### k6 Configuration

Edit `tests/load/k6.yml` for custom scenarios:

```yaml
options:
  scenarios:
    spike:
      executor: ramping-vus
      startVUs: 0
      stages:
        - duration: 2m
          target: 100
        - duration: 5m
          target: 1000
        - duration: 2m
          target: 0
```

## Performance Tuning

### API Server

```bash
# Increase worker threads
SERVER_WORKERS=8

# Increase connection pool
DB_MAX_CONNECTIONS=100
```

### Kafka

```bash
# Increase partitions
kafka-topics.sh --alter --topic football_simulator.events --partitions 12

# Tune producer
KAFKA_PRODUCER_BATCH_SIZE=16384
KAFKA_PRODUCER_LINGER_MS=10
```

### ClickHouse

```sql
-- Check memory usage
SELECT * FROM system.metrics WHERE metric LIKE '%Memory%';

-- Optimize table
OPTIMIZE TABLE engagement_events FINAL;
```

## Stress Testing

For maximum load testing:

```bash
# Maximum viewers
python match_simulator.py \
  --viewers 500000 \
  --batch-size 1000 \
  --concurrency 100

# Rapid fire events
python match_simulator.py \
  --duration 5 \
  --events-per-second 10000
```

## Expected Performance

| Metric | Target | Notes |
|--------|--------|-------|
| Events/second | 10,000+ | Sustained |
| P50 latency | <50ms | Normal load |
| P95 latency | <200ms | Normal load |
| P99 latency | <500ms | Spike load |
| Error rate | <0.1% | All scenarios |

## Troubleshooting

### High Latency

```bash
# Check API resources
docker stats

# Check ClickHouse
docker exec -it $(docker ps -q -f name=clickhouse) \
  clickhouse-client --query "SELECT * FROM system.processes"

# Increase consumer batch size
CONSUMER_BATCH_SIZE=2000
```

### Connection Errors

```bash
# Reduce concurrency
python match_simulator.py --concurrency 20

# Check Kafka connections
docker exec $(docker ps -q -f name=kafka) \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

### Memory Issues

```bash
# Restart services
docker compose restart

# Increase Docker memory
# Docker Desktop → Settings → Resources → Memory
```

## Best Practices

1. **Start small** - Begin with 1,000 viewers, scale up gradually
2. **Monitor constantly** - Watch Grafana during tests
3. **Reset between tests** - Clear data for accurate measurements
4. **Test incrementally** - Isolate issues by testing components separately
5. **Document results** - Record metrics for comparison

## Sample Test Plan

### Phase 1: Baseline (Day 1)
- 1,000 viewers, 5 minutes
- Establish baseline metrics

### Phase 2: Scale (Day 2)
- 10,000 viewers, 30 minutes
- Identify bottlenecks

### Phase 3: Stress (Day 3)
- 50,000 viewers, 60 minutes
- Test recovery under load

### Phase 4: Production (Day 4)
- 100,000 viewers, 90 minutes
- Full match simulation

### Phase 5: Spike (Day 5)
- 100,000 → 200,000 spike
- Test goal event handling
