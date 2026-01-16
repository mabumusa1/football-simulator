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

## Architecture at a Glance

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

## Quick Links

- [Getting Started](getting-started.md) - Get up and running in minutes
- [API Reference](api-reference.md) - Complete API documentation
- [Deployment Guide](deployment.md) - Deploy to production
- [Development Setup](development.md) - Set up your dev environment

## For Developers

If you want to **run this code locally**:

```bash
# Clone and open in VS Code with Dev Containers
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure
code .
# Click "Reopen in Container" when prompted
```

See [Development Setup](development.md) for detailed instructions.

## For API Users

If you want to **send traffic to this service**:

```bash
# Send a match event
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

See [API Reference](api-reference.md) for complete endpoint documentation.

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

## License

This project is proprietary software.
