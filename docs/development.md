---
layout: default
title: Development
nav_order: 6
---

# Development Setup

Set up your local development environment for Football Infrastructure.

## Prerequisites

- **Docker** 20.10+ and **Docker Compose** v2
- **Go** 1.21+ (for local development without containers)
- **Git**
- **VS Code** with Dev Containers extension (recommended)

## Option 1: VS Code Dev Container (Recommended)

The fastest way to get a fully configured development environment.

### Setup

1. Install [VS Code](https://code.visualstudio.com/)
2. Install [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
3. Open the repository in VS Code
4. Click "Reopen in Container" when prompted

### What You Get

The dev container automatically:
- Builds the development environment
- Starts all infrastructure services (Kafka, ClickHouse, Prometheus, Grafana)
- Installs Go tools (gopls, golangci-lint, dlv)
- Configures environment variables
- Forwards all necessary ports

### Forwarded Ports

| Port | Service | URL |
|------|---------|-----|
| 8080 | API Server | http://localhost:8080 |
| 8123 | ClickHouse HTTP | http://localhost:8123 |
| 9000 | ClickHouse Native | - |
| 9092 | Kafka External | - |
| 8081 | Kafka UI | http://localhost:8081 |
| 9090 | Prometheus | http://localhost:9090 |
| 9091 | Consumer Metrics | http://localhost:9091 |
| 3005 | Grafana | http://localhost:3005 |

### VS Code Extensions (Auto-installed)

- `golang.go` - Go language support
- `ms-azuretools.vscode-docker` - Docker support
- `esbenp.prettier-vscode` - Code formatting
- `redhat.vscode-yaml` - YAML support
- `cweijan.dbclient-jdbc` - Database client

## Option 2: Manual Setup

If you prefer not to use Dev Containers.

### 1. Start Infrastructure Services

```bash
cd football-infrastructure

# Copy environment file
cp .env.example .env

# Start services
cd infra/compose/dev
docker compose -f base.yml -f kafka.yml -f clickhouse.yml -f monitoring.yml up -d

# Verify services are running
docker compose ps
```

### 2. Install Go Dependencies

```bash
# API service
cd apps/api
go mod download

# Consumer service
cd ../consumer
go mod download
```

### 3. Run the Services

**Terminal 1 - API:**
```bash
cd apps/api
go run .
```

**Terminal 2 - Consumer:**
```bash
cd apps/consumer
go run .
```

### 4. Verify Everything Works

```bash
# Health check
curl http://localhost:8080/health

# Readiness check
curl http://localhost:8080/ready
```

## Project Structure

```
football-infrastructure/
├── .devcontainer/           # Dev container configuration
│   ├── devcontainer.json    # VS Code dev container settings
│   ├── Dockerfile           # Dev container image
│   └── health-check.sh      # Service health verification
├── .github/workflows/       # CI/CD pipelines
│   ├── ci.yml              # Lint, test, SonarCloud
│   └── deploy.yml          # Build and deploy
├── .vscode/                 # VS Code settings
│   ├── launch.json         # Debug configurations
│   └── tasks.json          # Build tasks
├── apps/
│   ├── api/                # Go API service
│   │   ├── main.go
│   │   ├── go.mod
│   │   ├── Dockerfile
│   │   ├── openapi.yaml    # API specification
│   │   └── internal/
│   │       ├── api/        # HTTP handlers, routes
│   │       ├── app/        # Configuration
│   │       ├── domain/     # Models, validation
│   │       ├── kafka/      # Kafka producer
│   │       └── repository/ # ClickHouse access
│   └── consumer/           # Go consumer service
│       ├── main.go
│       ├── go.mod
│       ├── Dockerfile
│       └── internal/
│           ├── app/        # Configuration
│           ├── domain/     # Models
│           ├── kafka/      # Kafka consumer
│           └── repository/ # ClickHouse writer
├── infra/
│   ├── aws/cloudformation/ # AWS IaC
│   ├── clickhouse/         # Database config & schema
│   ├── compose/            # Docker Compose files
│   │   ├── dev/           # Development
│   │   └── prod/          # Production
│   ├── grafana/           # Dashboards
│   └── prometheus/        # Monitoring config
├── scripts/               # Deployment scripts
├── tests/load/            # Load testing
└── docs/                  # This documentation
```

## Running Tests

### Unit Tests

```bash
# API tests
cd apps/api
go test -v ./...

# With coverage
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Consumer tests
cd apps/consumer
go test -v ./...
```

### Integration Tests

Integration tests run against the dev container services:

```bash
# Ensure services are running
docker compose ps

# Run tests
cd apps/api
go test -v -tags=integration ./...
```

## Debugging

### VS Code Debug Configurations

The repository includes pre-configured debug configurations in `.vscode/launch.json`:

1. **Debug API Server** - Launch API with breakpoints
2. **Debug Consumer** - Launch Consumer with breakpoints
3. **Debug All** - Launch both services

**To debug:**
1. Set breakpoints in your code
2. Press F5 or select Run → Start Debugging
3. Select the configuration you want

### Debug with Delve

```bash
# Install delve
go install github.com/go-delve/delve/cmd/dlv@latest

# Debug API
cd apps/api
dlv debug . -- --config /path/to/config

# Connect VS Code to running debugger
dlv debug --headless --listen=:2345 --api-version=2
```

## VS Code Tasks

Pre-configured tasks in `.vscode/tasks.json`:

| Task | Description | Shortcut |
|------|-------------|----------|
| Start API Server | Run `go run .` in apps/api | Ctrl+Shift+B |
| Start Consumer | Run `go run .` in apps/consumer | - |
| Start All Services | Run both in parallel | - |
| Load Test Setup | Create Python venv | - |
| Load Test (1K) | Quick test, 1K viewers | - |
| Load Test (100K) | Full test, 100K viewers | - |

**Run a task:**
- Press `Ctrl+Shift+P`
- Type "Tasks: Run Task"
- Select the task

## Linting

The project uses golangci-lint:

```bash
# Install
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.6

# Run linter
cd apps/api
golangci-lint run

cd apps/consumer
golangci-lint run
```

Configuration is in `.golangci.yml` in each app directory.

## Building Docker Images

```bash
# Build API image
docker build -f apps/api/Dockerfile -t football-api:dev .

# Build Consumer image
docker build -f apps/consumer/Dockerfile -t football-consumer:dev .

# Test locally
docker run -p 8080:8080 --env-file .env football-api:dev
```

## Environment Variables

Key development variables (see `.env.example` for full list):

```bash
# API Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8080
API_KEY=dev-api-key

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_PREFIX=football_simulator

# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=9000
CLICKHOUSE_DATABASE=football_simulator
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Consumer
CONSUMER_BATCH_SIZE=1000
CONSUMER_FLUSH_INTERVAL=5s

# Monitoring
PROMETHEUS_URL=http://prometheus:9090
GRAFANA_URL=http://grafana:3000
```

## Database Access

### ClickHouse CLI

```bash
# Connect via Docker
docker exec -it $(docker ps -q -f name=clickhouse) clickhouse-client

# Or with parameters
docker exec -it $(docker ps -q -f name=clickhouse) \
  clickhouse-client --database=football_simulator
```

### Common Queries

```sql
-- View tables
SHOW TABLES;

-- Count match events
SELECT count(*) FROM match_events;

-- View recent events
SELECT * FROM match_events ORDER BY timestamp DESC LIMIT 10;

-- Engagement summary
SELECT engagement_type, count(*) as cnt
FROM engagement_events
GROUP BY engagement_type;

-- Concurrent viewers
SELECT * FROM v_concurrent_viewers;
```

## Kafka Access

### Kafka UI

Open http://localhost:8081 to:
- View topics and messages
- Monitor consumer groups
- Inspect partitions

### Kafka CLI

```bash
# List topics
docker exec $(docker ps -q -f name=kafka) \
  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume messages
docker exec $(docker ps -q -f name=kafka) \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic football_simulator.events \
  --from-beginning

# Produce test message
docker exec -it $(docker ps -q -f name=kafka) \
  kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic football_simulator.events
```

## Monitoring

### Prometheus

- URL: http://localhost:9090
- Explore metrics
- Test PromQL queries

**Useful queries:**
```promql
# Request rate
rate(http_requests_total[5m])

# Request latency
histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))

# Events ingested
sum(rate(events_ingested_total[5m])) by (type)
```

### Grafana

- URL: http://localhost:3005
- Login: admin / admin
- Pre-configured dashboards for load testing

## Code Generation

### OpenAPI

The API specification is in `apps/api/openapi.yaml`. To generate client code:

```bash
# Install oapi-codegen
go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest

# Generate Go client
oapi-codegen -package client -generate client apps/api/openapi.yaml > client/client.go
```

## Git Workflow

### Commit Messages

Follow conventional commits:

```
feat: add engagement rate calculation
fix: resolve race condition in consumer
docs: update API reference
test: add unit tests for validation
chore: update dependencies
```

### Branch Naming

```
feature/add-engagement-api
bugfix/fix-kafka-timeout
hotfix/security-patch
```

### Pull Request Process

1. Create feature branch
2. Make changes
3. Run tests and linter
4. Push and create PR
5. CI runs automatically
6. Merge after approval

## Troubleshooting

### Services Won't Start

```bash
# Check Docker daemon
docker info

# Check port conflicts
lsof -i :8080
lsof -i :9092
lsof -i :9000

# View compose logs
cd infra/compose/dev
docker compose logs
```

### Go Module Issues

```bash
# Clear module cache
go clean -modcache

# Re-download dependencies
go mod download

# Tidy modules
go mod tidy
```

### Dev Container Issues

```bash
# Rebuild container
# Command Palette → "Dev Containers: Rebuild Container"

# Reset Docker
docker system prune -a

# Check Docker resources
docker stats
```

### Database Connection Failed

```bash
# Check ClickHouse is running
curl http://localhost:8123/ping

# Check health script
bash .devcontainer/health-check.sh

# View ClickHouse logs
docker compose logs clickhouse
```
