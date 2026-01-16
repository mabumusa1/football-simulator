---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started

Get Football Infrastructure running in under 5 minutes.

## Prerequisites

- **Docker** (20.10+) and **Docker Compose** (v2)
- **Git**
- **VS Code** with [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) (recommended)

## Quick Start (Dev Container)

The fastest way to get started is using VS Code Dev Containers:

```bash
# 1. Clone the repository
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure

# 2. Open in VS Code
code .

# 3. When prompted, click "Reopen in Container"
#    Or use Command Palette: "Dev Containers: Reopen in Container"
```

The dev container will automatically:
- Build the development environment
- Start Kafka, ClickHouse, Prometheus, and Grafana
- Install Go tools and dependencies
- Configure all environment variables

## Quick Start (Manual)

If you prefer not to use Dev Containers:

```bash
# 1. Clone the repository
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure

# 2. Copy environment file
cp .env.example .env

# 3. Start infrastructure services
cd infra/compose/dev
docker compose -f base.yml -f kafka.yml -f clickhouse.yml -f monitoring.yml up -d

# 4. Wait for services to be healthy (about 30 seconds)
docker compose ps

# 5. Build and run the API
cd ../../../apps/api
go build -o api .
./api

# 6. In another terminal, run the consumer
cd apps/consumer
go build -o consumer .
./consumer
```

## Verify Installation

Once running, verify all services are healthy:

```bash
# API health check
curl http://localhost:8080/health
# Expected: {"status":"healthy","timestamp":"..."}

# API readiness (checks Kafka + ClickHouse)
curl http://localhost:8080/ready
# Expected: {"status":"ready","checks":{"clickhouse":"healthy","kafka":"healthy"}}

# ClickHouse
curl http://localhost:8123/ping
# Expected: Ok.

# Prometheus
curl http://localhost:9090/-/healthy
# Expected: Prometheus Server is Healthy.
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| API | http://localhost:8080 | - |
| API Docs (Swagger) | http://localhost:8080/ | - |
| ClickHouse HTTP | http://localhost:8123 | default / (empty) |
| Kafka UI | http://localhost:8081 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3005 | admin / admin |

## Send Your First Event

```bash
# Set API key (from .env file)
export API_KEY="your-api-key"

# Send a match event
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "eventId": "'"$(uuidgen)"'",
    "matchId": "match-001",
    "eventType": "goal",
    "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
    "teamId": 1,
    "playerId": "player-10"
  }'
```

Expected response:
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "timestamp": "2024-01-15T14:30:00Z"
}
```

## Send Engagement Events

```bash
curl -X POST http://localhost:8080/api/engagements \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "events": [{
      "event_id": "'"$(uuidgen)"'",
      "match_id": "match-001",
      "user_id": "user-123",
      "session_id": "session-456",
      "engagement_type": "reaction",
      "engagement_subtype": "emoji_goal",
      "device_type": "mobile",
      "game_minute": 45,
      "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"
    }]
  }'
```

## Run a Load Test

Test the system with simulated traffic:

```bash
cd tests/load

# Set up Python environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run a quick test (1,000 viewers, 5 minutes)
python match_simulator.py \
  --api-url http://localhost:8080 \
  --api-key "$API_KEY" \
  --viewers 1000 \
  --duration 5
```

## View Metrics in Grafana

1. Open http://localhost:3005
2. Login with `admin` / `admin`
3. Navigate to Dashboards → k6 Load Test
4. Watch real-time metrics during load tests

## Common Issues

### Services won't start
```bash
# Check Docker is running
docker info

# Check port conflicts
lsof -i :8080  # API
lsof -i :9092  # Kafka
lsof -i :9000  # ClickHouse
```

### API returns 503 (Service Unavailable)
```bash
# Check if Kafka and ClickHouse are healthy
docker compose ps

# View logs
docker compose logs kafka
docker compose logs clickhouse
```

### Permission denied errors
```bash
# Ensure you're in the docker group
sudo usermod -aG docker $USER
# Log out and back in
```

## Next Steps

- [API Reference](api-reference.md) - Learn all available endpoints
- [Architecture](architecture.md) - Understand the system design
- [Development Setup](development.md) - Configure your IDE
- [Deployment Guide](deployment.md) - Deploy to production
