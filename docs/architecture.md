---
layout: default
title: Architecture
nav_order: 5
---

# Architecture

Football Infrastructure is designed for high-throughput, real-time event processing with analytics capabilities.

## System Overview

```
                                    ┌─────────────────────────────────────────────────────────────┐
                                    │                        AWS Cloud                             │
                                    │  ┌────────────────────────────────────────────────────────┐ │
                                    │  │                    Docker Swarm                         │ │
┌──────────────────┐                │  │                                                        │ │
│   Mobile Apps    │                │  │  ┌─────────┐    ┌─────────┐    ┌──────────┐          │ │
│   Web Clients    │───HTTPS───────▶│──│──│ Traefik │───▶│ go-api  │───▶│  Kafka   │          │ │
│   Smart TVs      │                │  │  │  (SSL)  │    │(3 repl) │    │ (KRaft)  │          │ │
│   100K+ viewers  │                │  │  └─────────┘    └─────────┘    └────┬─────┘          │ │
└──────────────────┘                │  │                                      │               │ │
                                    │  │                                      ▼               │ │
                                    │  │  ┌─────────┐    ┌─────────┐    ┌──────────┐          │ │
                                    │  │  │ Grafana │◀───│Promethe.│◀───│go-consum.│          │ │
                                    │  │  │         │    │         │    │(2 repl)  │          │ │
                                    │  │  └─────────┘    └─────────┘    └────┬─────┘          │ │
                                    │  │                                      │               │ │
                                    │  │                                      ▼               │ │
                                    │  │                              ┌──────────────┐        │ │
                                    │  │                              │  ClickHouse  │        │ │
                                    │  │                              │   (OLAP DB)  │        │ │
                                    │  │                              └──────────────┘        │ │
                                    │  └────────────────────────────────────────────────────────┘ │
                                    └─────────────────────────────────────────────────────────────┘
```

## Components

### API Service (go-api)

The API service handles all incoming HTTP requests from clients.

**Responsibilities:**
- Accept match events via REST API
- Accept viewer engagement events in batches
- Validate incoming data
- Produce events to Kafka topics
- Expose Prometheus metrics
- Serve OpenAPI documentation

**Technology:**
- Go 1.21
- chi router (HTTP)
- kafka-go (Kafka producer)
- Prometheus client

**Scaling:**
- 3 replicas in production
- Stateless design
- Rolling updates (1 at a time)

### Consumer Service (go-consumer)

The consumer processes events from Kafka and persists them to ClickHouse.

**Responsibilities:**
- Consume events from Kafka topics
- Batch events for efficient writes
- Write to ClickHouse database
- Handle retries and dead-letter queue
- Expose Prometheus metrics

**Technology:**
- Go 1.21
- kafka-go (Kafka consumer)
- clickhouse-go (database client)

**Configuration:**
- Batch size: 1,000 events (configurable)
- Flush interval: 5 seconds
- Max retries: 3

### Message Queue (Kafka)

Apache Kafka provides reliable, ordered message delivery.

**Configuration:**
- KRaft mode (no Zookeeper required)
- Single broker (scalable to multi-broker)
- 3 partitions per topic (default)
- 7-day retention

**Topics:**
| Topic | Purpose |
|-------|---------|
| `football_simulator.events` | Match events (goals, passes, fouls) |
| `football_simulator.engagements` | Viewer engagement events |
| `football_simulator.retry` | Failed events for retry |
| `football_simulator.dead` | Events that exceeded max retries |

### Analytics Database (ClickHouse)

ClickHouse provides fast OLAP queries for real-time analytics.

**Tables:**

| Table | Purpose | Engine |
|-------|---------|--------|
| `match_events` | Game events from the field | MergeTree |
| `engagement_events` | Viewer engagement tracking | MergeTree |
| `api_events` | API request/response logging | MergeTree |
| `active_sessions` | Concurrent viewer tracking | ReplacingMergeTree |

**Materialized Views:**
- `engagement_per_minute` - Engagement timeline
- `engagement_by_type` - Aggregated by engagement type
- `engagement_by_device` - Device/platform breakdown

**Analytics Views:**
- `v_concurrent_viewers` - Real-time viewer counts
- `v_engagement_rate` - Engagements per user
- `v_peak_engagement` - Engagement spikes
- `v_game_event_impact` - Game event correlation
- `v_viewer_retention` - Retention curves

### Reverse Proxy (Traefik)

Traefik handles SSL termination and load balancing.

**Features:**
- Automatic Let's Encrypt SSL certificates
- HTTP to HTTPS redirect
- Health-check based routing
- Docker Swarm service discovery
- Prometheus metrics

**Routes:**
- `api.domain.com` → go-api service
- `grafana.domain.com` → Grafana
- `prometheus.domain.com` → Prometheus (auth protected)

### Monitoring Stack

**Prometheus:**
- Scrapes metrics from all services
- 15-second scrape interval
- 15-day retention

**Grafana:**
- Pre-configured dashboards
- ClickHouse datasource
- Load test visualization

## Data Flow

### Match Event Flow

```
1. Client sends POST /api/events
2. API validates event structure
3. API produces to Kafka topic: football_simulator.events
4. Consumer batches events (1000 or 5s)
5. Consumer writes batch to ClickHouse match_events table
6. Materialized views update automatically
```

### Engagement Event Flow

```
1. Client sends POST /api/engagements (batch of events)
2. API validates each event
3. API produces to Kafka topic: football_simulator.engagements
4. Consumer batches events
5. Consumer writes to ClickHouse engagement_events table
6. Materialized views aggregate data in real-time
```

### Query Flow

```
1. Client sends GET /api/matches/{matchId}/metrics
2. API queries ClickHouse views
3. ClickHouse returns aggregated results
4. API formats and returns JSON response
```

## Deployment Architecture

### Development

```
Docker Compose (bridge network)
├── go-api (1 replica)
├── go-consumer (1 replica)
├── kafka (single broker)
├── clickhouse (single instance)
├── prometheus
├── grafana
└── kafka-ui (debugging)
```

### Production

```
Docker Swarm (overlay network)
├── traefik (1 replica, manager node)
├── go-api (3 replicas, rolling updates)
├── go-consumer (2 replicas)
├── kafka (1 broker, persistent volume)
├── clickhouse (1 instance, labeled node)
├── prometheus (1 replica, persistent volume)
└── grafana (1 replica, persistent volume)
```

### AWS Infrastructure

```
VPC (10.0.0.0/16)
└── Public Subnet (10.0.1.0/24)
    └── EC2 Instance (r5.xlarge)
        ├── Docker Swarm Manager
        ├── 300GB gp3 EBS (6000 IOPS)
        └── Elastic IP

ECR Repositories
├── go-api (with image scanning)
└── go-consumer (with image scanning)

IAM Roles
├── EC2 Instance Role (SSM, CloudWatch, ECR)
└── GitHub Actions Role (ECR push, SSM deploy)
```

## Scalability Considerations

### Current Limits (Single Node)

| Metric | Capacity |
|--------|----------|
| Concurrent viewers | 100K+ |
| Events per minute | 10K+ |
| API replicas | 3 |
| Consumer replicas | 2 |

### Scaling Strategies

**Horizontal (add more nodes):**
1. Add worker nodes to Swarm
2. Scale API replicas: `docker service scale football_go-api=10`
3. Scale consumer replicas for more Kafka partitions

**Vertical (bigger instance):**
- r5.2xlarge: 8 vCPU, 64GB RAM
- Increase ClickHouse memory limits
- Increase Kafka heap size

**Multi-Node Kafka:**
1. Add Kafka broker nodes
2. Increase topic partitions
3. Configure replication factor

**ClickHouse Cluster:**
1. Add ClickHouse shards
2. Configure distributed tables
3. Set up ZooKeeper for coordination

## Security

### Network Security

- All external traffic via HTTPS (Traefik)
- Internal services on overlay network
- Security groups restrict access by port
- SSH restricted by CIDR

### Authentication

- API endpoints require `X-API-Key` header
- Monitoring dashboards use basic auth
- AWS uses IAM roles (no access keys)
- GitHub Actions uses OIDC (no secrets in code)

### Container Security

- Non-root containers
- Read-only filesystems where possible
- Resource limits on all services
- ECR image scanning enabled

## Monitoring & Observability

### Metrics (Prometheus)

**API Metrics:**
- `http_requests_total` - Request count by method/path/status
- `http_request_duration_seconds` - Latency histogram
- `events_ingested_total` - Events by type
- `kafka_produce_errors_total` - Producer failures

**Consumer Metrics:**
- `events_consumed_total` - Events processed
- `batch_write_duration_seconds` - ClickHouse write latency
- `retry_count_total` - Retry attempts
- `dead_letter_total` - Failed events

### Logs

- Structured JSON logging (slog)
- CloudWatch Logs in production
- Request ID correlation
- Log levels: debug, info, warn, error

### Health Checks

| Endpoint | Purpose | Interval |
|----------|---------|----------|
| `/health` | Basic liveness | 30s |
| `/ready` | Dependency check | 30s |
| `/metrics` | Prometheus scrape | 15s |

## Failure Handling

### API Failures

- Request timeouts (30s)
- Circuit breaker on dependencies
- Graceful degradation (return 503)
- Retry with exponential backoff

### Kafka Failures

- Producer retries (3 attempts)
- Consumer group rebalancing
- Dead-letter queue for poison messages
- Partition replication (when multi-broker)

### ClickHouse Failures

- Connection pooling with health checks
- Batch retry on write failure
- Materialized view automatic rebuild
- Data TTL for storage management
