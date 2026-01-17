---
layout: default
title: Home
nav_order: 1
---

# Football Infrastructure

A high-performance, real-time football match event streaming and viewer engagement analytics platform designed to handle **100K+ concurrent viewers**.

## Overview

Football Infrastructure ingests game events (goals, passes, fouls, etc.) and correlates them with viewer engagement data (reactions, comments, shares) to provide real-time analytics for sports streaming platforms.

## Key Features

- **High Throughput**: Handle 10K+ events per minute
- **Real-time Analytics**: Sub-second latency for engagement metrics
- **Scalable Architecture**: Horizontal scaling via Docker Swarm
- **Production Ready**: SSL/TLS, monitoring, CI/CD included

## Architecture

```
┌─────────────┐     ┌─────────┐     ┌──────────────┐     ┌────────────┐
│   Clients   │────▶│   API   │────▶│    Kafka     │────▶│  Consumer  │
│  (100K+)    │     │  (Go)   │     │   (KRaft)    │     │   (Go)     │
└─────────────┘     └─────────┘     └──────────────┘     └────────────┘
                                                               │
                                                               ▼
┌─────────────┐     ┌─────────┐     ┌──────────────┐     ┌────────────┐
│   Grafana   │◀────│Prometheus│◀───│   Metrics    │     │ ClickHouse │
│             │     │         │     │              │     │   (OLAP)   │
└─────────────┘     └─────────┘     └──────────────┘     └────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| API & Consumer | Go 1.21 |
| Message Queue | Apache Kafka (KRaft) |
| Analytics DB | ClickHouse |
| Orchestration | Docker Swarm |
| Reverse Proxy | Traefik |
| Monitoring | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Cloud | AWS (CloudFormation) |

---

## Documentation

### For Developers

Start here if you want to **run this code locally** or **contribute**:

1. **[Getting Started](getting-started.md)** - Clone, dev container setup, run locally
2. **[Contributing](contributing.md)** - PR workflow, CI/CD, code quality
3. **[Architecture](architecture.md)** - System design and data flows

### For Operators

Start here if you want to **deploy to production**:

1. **[Deployment Guide](deployment.md)** - AWS CloudFormation, Docker Swarm, CI/CD
2. **[Configuration](configuration.md)** - Environment variables reference
3. **[Load Testing](load-testing.md)** - Simulate 100K viewers

### For API Users

Start here if you want to **send data to this service**:

1. **[API Reference](api-reference.md)** - Complete endpoint documentation with examples

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure

# Open in VS Code and start Dev Container
code .
# Click "Reopen in Container" when prompted
```

See [Getting Started](getting-started.md) for detailed instructions.

---

## Send Events

### Match Event

```bash
curl -X POST https://api.your-domain.com/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440000",
    "matchId": "match-2024-001",
    "eventType": "goal",
    "timestamp": "2024-01-15T14:30:00Z",
    "teamId": 1
  }'
```

### Engagement Event

```bash
curl -X POST https://api.your-domain.com/api/engagements \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your-api-key" \
  -d '{
    "events": [{
      "event_id": "550e8400-e29b-41d4-a716-446655440001",
      "match_id": "match-2024-001",
      "user_id": "user-456",
      "session_id": "session-789",
      "engagement_type": "reaction",
      "engagement_subtype": "emoji_goal",
      "game_minute": 45,
      "device_type": "mobile",
      "timestamp": "2024-01-15T14:30:01Z"
    }]
  }'
```

See [API Reference](api-reference.md) for complete endpoint documentation.

---

## Repository Structure

```
football-infrastructure/
├── apps/
│   ├── api/          # Go REST API service
│   └── consumer/     # Go Kafka consumer service
├── infra/
│   ├── aws/          # CloudFormation templates
│   ├── compose/      # Docker Compose files
│   ├── clickhouse/   # Database schema
│   ├── prometheus/   # Monitoring config
│   └── grafana/      # Dashboards
├── scripts/          # Deployment scripts
├── tests/load/       # Load testing tools
└── docs/             # This documentation
```

---

## License

This project is proprietary software.
