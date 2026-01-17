# Football Infrastructure

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=bugs)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=coverage)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=mabumusa1_football-simulator&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=mabumusa1_football-simulator)

A high-performance, real-time football match event streaming and viewer engagement analytics platform designed to handle **100K+ concurrent viewers**.

**[View Documentation](https://mabumusa1.github.io/football-simulator/)**

## Quick Start

```bash
# Clone the repository
git clone https://github.com/mabumusa1/football-infrastructure.git
cd football-infrastructure

# Open in VS Code with Dev Containers
code .
# Click "Reopen in Container" when prompted
```

## Documentation

All documentation is in the [`docs/`](docs/) folder:

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Clone, dev container setup, run locally |
| [Contributing](docs/contributing.md) | PR workflow, CI/CD, code quality |
| [Deployment](docs/deployment.md) | AWS CloudFormation, Docker Swarm |
| [API Reference](docs/api-reference.md) | Complete endpoint documentation |
| [Architecture](docs/architecture.md) | System design and data flows |
| [Configuration](docs/configuration.md) | Environment variables reference |
| [Load Testing](docs/load-testing.md) | Simulate 100K viewers |

## Tech Stack

- **Go 1.21** - API and Consumer services
- **Apache Kafka** (KRaft) - Message queue
- **ClickHouse** - OLAP analytics database
- **Docker Swarm** - Container orchestration
- **Traefik** - Reverse proxy with SSL
- **Prometheus + Grafana** - Monitoring
- **AWS CloudFormation** - Infrastructure as Code

## License

This project is proprietary software.
