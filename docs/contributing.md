---
layout: default
title: Contributing
nav_order: 3
---

# Contributing

How to contribute to Football Infrastructure: branching, commits, pull requests, and CI/CD pipeline.

## Git Workflow

### Branch Naming

```
feature/add-engagement-api
bugfix/fix-kafka-timeout
hotfix/security-patch
docs/update-api-reference
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add engagement rate calculation
fix: resolve race condition in consumer
docs: update API reference
test: add unit tests for validation
chore: update dependencies
refactor: simplify batch processing logic
```

**Examples:**

```bash
# Feature
git commit -m "feat: add real-time viewer count endpoint"

# Bug fix
git commit -m "fix: handle nil pointer in event validation"

# Breaking change
git commit -m "feat!: change engagement event schema

BREAKING CHANGE: engagement_type field renamed to type"
```

---

## Pull Request Process

### 1. Create Feature Branch

```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature-name
```

### 2. Make Changes

- Write code following project conventions
- Add tests for new functionality
- Update documentation if needed

### 3. Run Quality Checks Locally

```bash
# Run linter
cd apps/api && golangci-lint run
cd apps/consumer && golangci-lint run

# Run tests
cd apps/api && go test -v ./...
cd apps/consumer && go test -v ./...

# Check formatting
gofmt -d .
```

### 4. Push and Create PR

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub with:
- Clear title following commit convention
- Description of changes
- Link to related issues (if any)

### 5. CI Runs Automatically

The CI pipeline will:
1. Run linter (`golangci-lint`)
2. Run unit tests with coverage
3. Upload results to SonarCloud

### 6. Review and Merge

- Address review comments
- Ensure CI passes
- Merge after approval

---

## CI/CD Pipeline

### CI Workflow (`.github/workflows/ci.yml`)

Triggered on:
- Push to `main`
- Pull requests to `main`

**Jobs:**

| Job | Description |
|-----|-------------|
| `lint` | Run golangci-lint on both services |
| `test` | Run unit tests with coverage |
| `sonarcloud` | Upload coverage to SonarCloud |

### Deploy Workflow (`.github/workflows/deploy.yml`)

Triggered on:
- Push to `main` (deploys `latest` tag)
- Version tags (`v*.*.*`)
- Manual dispatch

**Jobs:**

| Job | Description |
|-----|-------------|
| `build` | Build Docker images for go-api and go-consumer |
| `push` | Push images to ECR |
| `deploy` | Update services via SSM command |
| `release` | Create GitHub release (for version tags) |

### Creating a Release

```bash
# Create version tag
git tag v1.0.0
git push origin v1.0.0
```

This triggers:
1. Docker images built and tagged with `v1.0.0`
2. Images pushed to ECR
3. Services updated on EC2
4. GitHub release created with changelog

---

## Code Quality

### Linting

The project uses [golangci-lint](https://golangci-lint.run/) with configuration in `.golangci.yml`:

```bash
# Install
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run
golangci-lint run

# Run with auto-fix
golangci-lint run --fix
```

### Testing

```bash
# Run all tests
go test -v ./...

# Run with coverage
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run specific test
go test -v -run TestEventValidation ./...
```

### Code Coverage

Coverage reports are uploaded to [SonarCloud](https://sonarcloud.io) on every CI run. View the project dashboard for:
- Coverage percentage
- Code smells
- Bugs and vulnerabilities
- Technical debt

---

## Project Structure

Understanding the codebase:

```
apps/
├── api/                 # REST API service
│   ├── main.go          # Entry point
│   ├── Dockerfile       # Container build
│   ├── openapi.yaml     # API specification
│   └── internal/
│       ├── api/         # HTTP handlers, routes, middleware
│       ├── app/         # Configuration, initialization
│       ├── domain/      # Models, validation
│       ├── kafka/       # Kafka producer
│       └── repository/  # ClickHouse queries
└── consumer/            # Kafka consumer service
    ├── main.go
    ├── Dockerfile
    └── internal/
        ├── app/         # Configuration
        ├── domain/      # Models
        ├── kafka/       # Kafka consumer
        └── repository/  # ClickHouse writer
```

### Key Patterns

**Configuration** (`internal/app/config.go`):
- Environment variables with defaults
- Type-safe configuration struct

**HTTP Handlers** (`internal/api/`):
- chi router for routing
- Middleware for auth, logging, recovery
- JSON request/response handling

**Kafka** (`internal/kafka/`):
- kafka-go library
- Batch producers and consumers
- Retry and dead-letter handling

**Repository** (`internal/repository/`):
- clickhouse-go library
- Batch inserts for performance
- Prepared statements

---

## Debugging

### VS Code Debug Configurations

Pre-configured in `.vscode/launch.json`:

1. **Debug API Server** - Launch API with breakpoints
2. **Debug Consumer** - Launch Consumer with breakpoints
3. **Debug All** - Launch both services

**To debug:**
1. Set breakpoints in code
2. Press F5 or Run → Start Debugging
3. Select configuration

### Debug with Delve

```bash
# Install
go install github.com/go-delve/delve/cmd/dlv@latest

# Debug API
cd apps/api
dlv debug .

# Debug with headless mode (for VS Code)
dlv debug --headless --listen=:2345 --api-version=2
```

### View Logs

```bash
# In dev container
docker compose logs -f go-api
docker compose logs -f go-consumer

# Service-specific
docker compose logs kafka
docker compose logs clickhouse
```

---

## Common Tasks

### Adding a New API Endpoint

1. Define in `apps/api/openapi.yaml`
2. Add handler in `apps/api/internal/api/handlers.go`
3. Add route in `apps/api/internal/api/routes.go`
4. Add tests in `apps/api/internal/api/handlers_test.go`

### Adding a New Kafka Topic

1. Add topic name to `internal/app/config.go`
2. Update producer/consumer to use new topic
3. Update `KAFKA_*` environment variables
4. Add ClickHouse table if needed

### Adding a New ClickHouse Table

1. Create migration in `infra/clickhouse/migrations/`
2. Add table to `infra/clickhouse/schema.sql`
3. Update repository code to use new table
4. Run migration in dev container

### Updating Dependencies

```bash
cd apps/api
go get -u ./...
go mod tidy

cd apps/consumer
go get -u ./...
go mod tidy
```

---

## Troubleshooting

### CI Lint Failures

```bash
# Run locally to see issues
golangci-lint run

# Common fixes
gofmt -w .           # Format code
go mod tidy          # Clean dependencies
```

### CI Test Failures

```bash
# Run tests locally
go test -v ./...

# Run specific failing test
go test -v -run TestName ./path/to/package
```

### Build Failures

```bash
# Check Go version matches (1.21+)
go version

# Clear module cache
go clean -modcache
go mod download
```
