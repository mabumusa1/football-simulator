---
layout: default
title: Configuration
nav_order: 7
---

# Configuration Reference

All environment variables used by Football Infrastructure services, organized by component.

## Overview

Configuration is handled through environment variables. In development, the Dev Container sets all required values automatically. In production, values are configured via CloudFormation parameters and the deployment script.

---

## API Service

The Go API service (`apps/api`) accepts these environment variables:

### Server Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_HOST` | `0.0.0.0` | HTTP server bind address |
| `SERVER_PORT` | `8080` | HTTP server port |
| `SERVER_READ_TIMEOUT` | `10s` | Request read timeout |
| `SERVER_WRITE_TIMEOUT` | `10s` | Response write timeout |
| `SERVER_IDLE_TIMEOUT` | `60s` | Keep-alive timeout |

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `API_KEY` | - | **Required**. API key for protected endpoints |

### Kafka Producer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `KAFKA_TOPIC_EVENTS` | `football_simulator.events` | Match events topic |
| `KAFKA_TOPIC_ENGAGEMENTS` | `football_simulator.engagements` | Engagement events topic |
| `KAFKA_PRODUCER_TIMEOUT` | `10s` | Producer timeout |

### ClickHouse (for metrics queries)

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse server host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native port |
| `CLICKHOUSE_DATABASE` | `football_simulator` | Database name |
| `CLICKHOUSE_USER` | `default` | Database user |
| `CLICKHOUSE_PASSWORD` | - | Database password |

---

## Consumer Service

The Go consumer service (`apps/consumer`) processes events from Kafka and writes to ClickHouse:

### Kafka Consumer

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker addresses |
| `KAFKA_TOPIC_EVENTS` | `football_simulator.events` | Match events topic |
| `KAFKA_TOPIC_ENGAGEMENTS` | `football_simulator.engagements` | Engagement events topic |
| `KAFKA_TOPIC_RETRY` | `football_simulator.retry` | Retry queue topic |
| `KAFKA_TOPIC_DEAD` | `football_simulator.dead` | Dead letter topic |
| `CONSUMER_GROUP` | `football_simulator-consumers` | Consumer group name |

### Event Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `CONSUMER_BATCH_SIZE` | `500` | Events per batch |
| `CONSUMER_FLUSH_INTERVAL` | `1s` | Max time before flush |
| `CONSUMER_MAX_RETRIES` | `3` | Retry attempts |
| `CONSUMER_RETRY_BACKOFF` | `1s` | Backoff between retries |
| `CONSUMER_WORKER_COUNT` | `2` | Parallel workers |

### High-Volume Engagement Processing

| Variable | Default | Description |
|----------|---------|-------------|
| `ENGAGEMENT_BATCH_SIZE` | `5000` | Engagements per batch |
| `ENGAGEMENT_FLUSH_INTERVAL` | `200ms` | Max time before flush |
| `ENGAGEMENT_WORKER_COUNT` | `16` | Parallel workers |

### ClickHouse Writer

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse server host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native port |
| `CLICKHOUSE_DATABASE` | `football_simulator` | Database name |
| `CLICKHOUSE_USER` | `default` | Database user |
| `CLICKHOUSE_PASSWORD` | - | Database password |

---

## Kafka (KRaft Mode)

Apache Kafka runs in KRaft mode (no Zookeeper). These are container-level settings:

### Broker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_NODE_ID` | `1` | Broker node ID |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Node roles |
| `CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` | Cluster identifier |

### Listeners

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_LISTENERS` | `PLAINTEXT://0.0.0.0:29092,...` | Listener bindings |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:29092,...` | Advertised addresses |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | `PLAINTEXT` | Inter-broker listener |

### Topic Defaults

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `true` | Auto-create topics |
| `KAFKA_NUM_PARTITIONS` | `3` | Default partitions |
| `KAFKA_LOG_RETENTION_HOURS` | `168` | Log retention (7 days) |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1` | Offsets replication |

---

## ClickHouse

ClickHouse OLAP database settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_DB` | `football_simulator` | Database to create |
| `CLICKHOUSE_USER` | `default` | Admin user |
| `CLICKHOUSE_PASSWORD` | - | Admin password |
| `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` | `1` | Enable SQL user management |

---

## Grafana

Grafana monitoring dashboard:

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `GF_SECURITY_ADMIN_USER` | `admin` | Admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | `admin` | Admin password |
| `GF_USERS_ALLOW_SIGN_UP` | `false` | Allow user registration |

### Anonymous Access

| Variable | Default | Description |
|----------|---------|-------------|
| `GF_AUTH_ANONYMOUS_ENABLED` | `false` | Allow anonymous access |
| `GF_AUTH_ANONYMOUS_ORG_ROLE` | `Viewer` | Anonymous user role |

### Server (Production)

| Variable | Default | Description |
|----------|---------|-------------|
| `GF_SERVER_PROTOCOL` | `http` | Server protocol |
| `GF_SERVER_ROOT_URL` | `https://grafana.example.com` | Public URL |
| `GF_SECURITY_COOKIE_SECURE` | `true` | Secure cookies |

### Plugins

| Variable | Default | Description |
|----------|---------|-------------|
| `GF_INSTALL_PLUGINS` | `grafana-clickhouse-datasource` | Plugins to install |

---

## Traefik (Production)

Traefik reverse proxy settings for production:

| Variable | Description |
|----------|-------------|
| `DOMAIN` | Your domain name (e.g., `football.example.com`) |
| `LETSENCRYPT_EMAIL` | Email for SSL certificate notifications |
| `TRAEFIK_AUTH` | Basic auth for dashboard (htpasswd format) |

### Generating Basic Auth

```bash
# Install htpasswd (if needed)
# Ubuntu: apt-get install apache2-utils
# macOS: brew install httpd

# Generate password hash
htpasswd -nb admin your_secure_password

# Output: admin:$apr1$xyz...
# Use this value for TRAEFIK_AUTH (escape $ as $$ in compose files)
```

---

## Prometheus (Production)

| Variable | Description |
|----------|-------------|
| `PROMETHEUS_AUTH` | Basic auth for web UI (htpasswd format) |

---

## Docker Registry (Production)

| Variable | Default | Description |
|----------|---------|-------------|
| `REGISTRY` | - | ECR registry URL (from CloudFormation) |
| `VERSION` | `latest` | Image version tag |

---

## Development vs Production

### Development (Dev Container)

All values are pre-configured in the Dev Container. Services communicate via Docker network DNS names (`kafka`, `clickhouse`, etc.).

### Production (AWS)

Values are set by the CloudFormation deployment:

1. **CloudFormation parameters** configure domain, SSL, instance type
2. **User data script** generates credentials and writes `/opt/football/.env`
3. **Deploy script** uses environment variables for Docker Swarm services

See [Deployment Guide](deployment.md) for production setup.

---

## Configuration Loading

Both Go services load configuration at startup using environment variables with defaults:

```go
// apps/api/internal/app/config.go
// apps/consumer/internal/app/config.go

func getEnv(key, defaultValue string) string {
    if value, exists := os.LookupEnv(key); exists {
        return value
    }
    return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
    if value, exists := os.LookupEnv(key); exists {
        if duration, err := time.ParseDuration(value); err == nil {
            return duration
        }
    }
    return defaultValue
}
```

Duration values support Go duration format: `10s`, `5m`, `1h`, `200ms`.
