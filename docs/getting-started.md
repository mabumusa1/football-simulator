---
layout: default
title: Getting Started
nav_order: 2
---

# Getting Started

Get Football Infrastructure running locally in under 5 minutes.

## Prerequisites

- **Docker** (20.10+) and **Docker Compose** (v2)
- **Git**
- **VS Code** with [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure
```

### 2. Open in VS Code

```bash
code .
```

### 3. Start Dev Container

When VS Code opens, you'll see a prompt: **"Reopen in Container"** - click it.

Or use Command Palette (`Ctrl+Shift+P` / `Cmd+Shift+P`):
- Type: `Dev Containers: Reopen in Container`

The dev container will automatically:
- Build the development environment with Go 1.21
- Start all infrastructure services (Kafka, ClickHouse, Prometheus, Grafana)
- Install Go tools (gopls, golangci-lint, dlv)
- Configure all environment variables
- Forward all necessary ports

**First build takes 2-3 minutes.** Subsequent opens are faster.

---

## Verify Installation

Once the container is running, verify all services are healthy:

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

---

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| API | http://localhost:8080 | - |
| API Docs (Swagger) | http://localhost:8080/ | - |
| ClickHouse HTTP | http://localhost:8123 | default / (empty) |
| Kafka UI | http://localhost:8081 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3005 | admin / admin |

---

## Send Your First Event

### Match Event

```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{
    "eventId": "'"$(uuidgen || cat /proc/sys/kernel/random/uuid)"'",
    "matchId": "match-001",
    "eventType": "goal",
    "timestamp": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
    "teamId": 1,
    "playerId": "player-10"
  }'
```

**Expected response:**
```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted",
  "timestamp": "2024-01-15T14:30:00Z"
}
```

### Engagement Event

```bash
curl -X POST http://localhost:8080/api/engagements \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-api-key" \
  -d '{
    "events": [{
      "event_id": "'"$(uuidgen || cat /proc/sys/kernel/random/uuid)"'",
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

---

## Project Structure

```
football-infrastructure/
├── .devcontainer/           # Dev container configuration
├── .github/workflows/       # CI/CD pipelines
├── .vscode/                 # VS Code settings, debug configs
├── apps/
│   ├── api/                 # Go REST API service
│   └── consumer/            # Go Kafka consumer service
├── infra/
│   ├── aws/                 # CloudFormation templates
│   ├── compose/             # Docker Compose files
│   ├── clickhouse/          # Database schema
│   ├── prometheus/          # Monitoring config
│   └── grafana/             # Dashboards
├── scripts/                 # Deployment scripts
├── tests/load/              # Load testing tools
└── docs/                    # Documentation
```

---

## Running Services

The API and Consumer services start automatically in the dev container.

### Manual Start (if needed)

```bash
# Terminal 1 - API
cd apps/api && go run .

# Terminal 2 - Consumer
cd apps/consumer && go run .
```

### VS Code Tasks

Use pre-configured tasks via Command Palette (`Ctrl+Shift+P`):
- `Tasks: Run Task` → `Start API Server`
- `Tasks: Run Task` → `Start Consumer`
- `Tasks: Run Task` → `Start All Services`

---

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

### Dev Container won't build

```bash
# Rebuild container
# Command Palette → "Dev Containers: Rebuild Container"

# If that fails, reset Docker
docker system prune -a
```

### Permission denied errors

```bash
# Ensure you're in the docker group
sudo usermod -aG docker $USER
# Log out and back in
```

---

## Next Steps

- [API Reference](api-reference.md) - Learn all available endpoints
- [Contributing](contributing.md) - PR workflow and CI/CD
- [Architecture](architecture.md) - Understand the system design
- [Deployment Guide](deployment.md) - Deploy to AWS
- [Load Testing](load-testing.md) - Simulate 100K viewers
