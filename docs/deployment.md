---
layout: default
title: Deployment
nav_order: 4
---

# Deployment Guide

Deploy Football Infrastructure to production on AWS using Docker Swarm.

## Overview

The production deployment uses:
- **AWS EC2** - Single node running Docker Swarm
- **AWS ECR** - Container registry for Docker images
- **CloudFormation** - Infrastructure as Code
- **GitHub Actions** - CI/CD pipeline
- **Traefik** - SSL termination and load balancing

## Prerequisites

- AWS account with appropriate permissions
- AWS CLI configured
- Domain name with DNS access
- GitHub repository with Actions enabled

## Architecture

```
AWS Cloud
├── VPC (10.0.0.0/16)
│   └── Public Subnet (10.0.1.0/24)
│       └── EC2 Instance (r5.xlarge)
│           ├── Docker Swarm Manager
│           ├── 300GB gp3 EBS (6000 IOPS)
│           └── Elastic IP
├── ECR Repositories
│   ├── go-api
│   └── go-consumer
└── IAM Roles
    ├── EC2 Instance Role
    └── GitHub Actions Role (OIDC)
```

## Step 1: Create EC2 Key Pair

```bash
# Create key pair
aws ec2 create-key-pair \
  --key-name football-key \
  --query 'KeyMaterial' \
  --output text > football-key.pem

# Secure the key
chmod 400 football-key.pem
```

## Step 2: Configure Parameters

Edit the CloudFormation parameters file:

```bash
# Copy and edit parameters
cp infra/aws/cloudformation/parameters/dev.json infra/aws/cloudformation/parameters/prod.json
```

**File: `infra/aws/cloudformation/parameters/prod.json`**

```json
[
  {
    "ParameterKey": "Domain",
    "ParameterValue": "football.example.com"
  },
  {
    "ParameterKey": "LetsEncryptEmail",
    "ParameterValue": "admin@example.com"
  },
  {
    "ParameterKey": "KeyPairName",
    "ParameterValue": "football-key"
  },
  {
    "ParameterKey": "InstanceType",
    "ParameterValue": "r5.xlarge"
  },
  {
    "ParameterKey": "SSHAccessCIDR",
    "ParameterValue": "YOUR_IP/32"
  },
  {
    "ParameterKey": "Environment",
    "ParameterValue": "production"
  }
]
```

**Instance Type Recommendations:**

| Type | vCPU | RAM | Use Case |
|------|------|-----|----------|
| t3.xlarge | 4 | 16GB | Testing only |
| t3.2xlarge | 8 | 32GB | Budget production |
| r5.xlarge | 4 | 32GB | Recommended |
| r5.2xlarge | 8 | 64GB | High headroom |
| r6i.xlarge | 4 | 32GB | Latest generation |

## Step 3: Deploy CloudFormation Stack

```bash
# Validate template
aws cloudformation validate-template \
  --template-body file://infra/aws/cloudformation/single-node-swarm.yaml

# Create stack
aws cloudformation create-stack \
  --stack-name football-prod \
  --template-body file://infra/aws/cloudformation/single-node-swarm.yaml \
  --parameters file://infra/aws/cloudformation/parameters/prod.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1

# Wait for completion (10-15 minutes)
aws cloudformation wait stack-create-complete \
  --stack-name football-prod \
  --region us-east-1

# Get outputs
aws cloudformation describe-stacks \
  --stack-name football-prod \
  --query 'Stacks[0].Outputs' \
  --output table
```

**Key Outputs:**

| Output | Description |
|--------|-------------|
| `InstancePublicIp` | Elastic IP address |
| `InstanceId` | EC2 instance ID |
| `ECRRegistry` | ECR registry URL |
| `GitHubActionsRoleArn` | IAM role for GitHub |

## Step 4: Configure DNS

Create DNS A records pointing to the Elastic IP:

```
api.football.example.com      → <Elastic IP>
grafana.football.example.com  → <Elastic IP>
prometheus.football.example.com → <Elastic IP>
traefik.football.example.com  → <Elastic IP>
```

Verify DNS propagation:
```bash
dig api.football.example.com
```

## Step 5: Configure GitHub Repository Variables

Add these **repository variables** (not secrets) to your GitHub repository:

**Settings → Secrets and variables → Actions → Variables tab → New repository variable**

| Variable | Value | Source |
|----------|-------|--------|
| `AWS_REGION` | us-east-1 | Your AWS region |
| `AWS_ACCOUNT_ID` | 123456789012 | CloudFormation output |
| `AWS_ROLE_ARN` | arn:aws:iam::...:role/football-dev-github-actions | GitHubActionsRoleArn output |
| `EC2_INSTANCE_ID` | i-0123456789abcdef0 | InstanceId output |

**Using GitHub CLI:**

```bash
# Set all variables at once
gh variable set AWS_REGION --body "us-east-1"
gh variable set AWS_ACCOUNT_ID --body "123456789012"
gh variable set AWS_ROLE_ARN --body "arn:aws:iam::123456789012:role/football-dev-github-actions"
gh variable set EC2_INSTANCE_ID --body "i-0123456789abcdef0"

# Verify
gh variable list
```

**Optional Secrets (Settings → Secrets → Actions):**

| Secret | Value | Source |
|--------|-------|--------|
| `SONAR_TOKEN` | sqa_... | From SonarCloud (optional) |

## Step 6: Deploy with GitHub Actions

### Creating a Release (Recommended)

The recommended way to deploy is by creating a GitHub release, which triggers the CI/CD pipeline:

```bash
# Create a new release using GitHub CLI
gh release create v1.0.0 \
  --target main \
  --title "Release v1.0.0" \
  --notes "Initial production release"
```

This will:
1. ✅ Validate Docker Compose configuration
2. ✅ Build API and Consumer images in parallel
3. ✅ Push images to ECR with version tag (e.g., `v1.0.0`)
4. ✅ Deploy to EC2 via AWS SSM
5. ✅ Run health checks
6. ✅ Automatic rollback if health checks fail
7. ✅ Create GitHub release with changelog

### Monitor Deployment Progress

```bash
# Watch the deployment in real-time
gh run list --workflow=deploy.yml --limit 1
gh run watch <run-id>

# View deployment logs
gh run view <run-id> --log

# View failed step logs
gh run view <run-id> --log-failed
```

### Manual Workflow Dispatch

You can also trigger deployments manually:

```bash
# Deploy with custom version
gh workflow run deploy.yml \
  -f version=v1.0.1 \
  -f deploy_infra=false

# Deploy with infrastructure services
gh workflow run deploy.yml \
  -f version=v1.0.2 \
  -f deploy_infra=true
```

### Rollback to Previous Version

If a deployment fails or you need to rollback:

```bash
# Using the rollback workflow
gh workflow run rollback.yml \
  -f version=v1.0.0 \
  -f confirm=ROLLBACK

# Monitor rollback
gh run list --workflow=rollback.yml --limit 1
gh run watch <run-id>
```

### Manual Deployment (SSH)

For troubleshooting or initial setup, you can deploy manually via SSH:

```bash
# SSH into instance
ssh -i football-key.pem ec2-user@<elastic-ip>

# Navigate to app directory
cd /opt/football/app

# Pull latest code
git pull origin main

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <ecr-registry>

# Update version in .env
sed -i 's/^VERSION=.*/VERSION=v1.0.0/' .env

# Deploy specific services
./scripts/deploy.sh go-api go-consumer

# Or deploy all services
./scripts/deploy.sh
```

## Step 7: Verify Deployment

```bash
# Check Swarm services
docker service ls

# Expected output:
# ID     NAME                    MODE         REPLICAS   IMAGE
# xxx    football-simulator_traefik       replicated   1/1        traefik:3.6.7
# xxx    football-simulator_kafka         replicated   1/1        apache/kafka:4.1.1
# xxx    football-simulator_clickhouse    replicated   1/1        clickhouse/clickhouse-server:25...
# xxx    football-simulator_go-api        replicated   3/3        <ecr>/go-api:v1.0.0
# xxx    football-simulator_go-consumer   replicated   3/3        <ecr>/go-consumer:v1.0.0
# xxx    football-simulator_prometheus    replicated   1/1        prom/prometheus:v3.9.1
# xxx    football-simulator_grafana       replicated   1/1        grafana/grafana:11.4.0

# Test API
curl https://api.football.example.com/health

# Test with API key
curl https://api.football.example.com/api/events \
  -H "X-API-Key: <your-api-key>" \
  -H "Content-Type: application/json" \
  -d '{"eventId":"test","matchId":"m1","eventType":"goal","timestamp":"2024-01-15T00:00:00Z","teamId":1}'
```

## Service Access

| Service | URL | Auth |
|---------|-----|------|
| API | https://api.domain.com | X-API-Key header |
| Grafana | https://grafana.domain.com | admin / (from credentials.txt) |
| Prometheus | https://prometheus.domain.com | Basic auth |
| Traefik | https://traefik.domain.com | Basic auth |

**Get credentials:**
```bash
cat /opt/football/credentials.txt
```

## CI/CD Pipeline

The GitHub Actions pipeline (`.github/workflows/deploy.yml`) provides a complete CI/CD solution:

### Pipeline Jobs

| Job | Description | Duration |
|-----|-------------|----------|
| **Validate** | Validates Docker Compose files and Dockerfiles | ~5s |
| **Build** | Parallel builds for go-api and go-consumer | ~20-90s |
| **Deploy** | Deploys to EC2 via AWS SSM | ~60s |
| **Health Check** | Verifies services are healthy | ~30-60s |
| **Release** | Creates GitHub release (for tags only) | ~5s |

### Features

- **Parallel Builds**: API and Consumer build simultaneously
- **Docker Layer Caching**: Uses GitHub Actions cache for faster builds
- **Immutable Tags**: Each version gets a unique tag (no `latest` overwrites)
- **Health Checks**: Verifies deployment via container exec
- **Auto Rollback**: Rolls back to previous version if health checks fail
- **Infrastructure Sync**: Syncs compose files from git before deploy

### Triggers

| Trigger | Version Tag | Deployment |
|---------|-------------|------------|
| Tag `v*.*.*` | Tag name (e.g., `v1.0.0`) | Full deploy + Release |
| Manual dispatch | Custom or SHA-based | Optional |

### ECR Tag Immutability

The ECR repositories use **immutable tags** for security. This means:
- Each version tag can only be pushed once
- No `latest` tag is used (prevents overwrites)
- Use semantic versioning: `v1.0.0`, `v1.0.1`, `v1.1.0`, etc.

```bash
# Check existing tags
aws ecr describe-images \
  --repository-name football-dev/go-api \
  --query 'imageDetails[*].imageTags' \
  --output table
```

## Scaling

### Scale API Service

```bash
# Scale up
docker service scale football-simulator_go-api=5

# Scale down
docker service scale football-simulator_go-api=2
```

### Scale Consumer Service

```bash
docker service scale football-simulator_go-consumer=4
```

### Add Worker Node

1. Launch another EC2 instance
2. Install Docker
3. Join the swarm:
```bash
docker swarm join --token <worker-token> <manager-ip>:2377
```

## Updating

### Rolling Update (Zero Downtime)

```bash
# Update go-api service
docker service update \
  --image <ecr>/go-api:v1.1.0 \
  football-simulator_go-api

# Update go-consumer
docker service update \
  --image <ecr>/go-consumer:v1.1.0 \
  football-simulator_go-consumer
```

### Full Stack Redeploy

```bash
cd /opt/football/app
git pull
./scripts/deploy.sh
```

## Monitoring

### View Service Logs

```bash
# API logs
docker service logs football-simulator_go-api --tail 100 -f

# Consumer logs
docker service logs football-simulator_go-consumer --tail 100

# All services
docker service logs football-simulator_kafka
docker service logs football-simulator_clickhouse
docker service logs football-simulator_traefik
```

### Grafana Dashboards

1. Open https://grafana.your-domain.com
2. Navigate to Dashboards
3. View pre-configured dashboards:
   - System Metrics
   - API Performance

### CloudWatch Logs

Logs are also available in CloudWatch:
- `/football-prod/user-data` - EC2 setup logs
- `/football-prod/docker` - Docker/service logs

## Backup & Recovery

### ClickHouse Data

```bash
# Create backup
docker exec $(docker ps -q -f name=clickhouse) \
  clickhouse-client --query "BACKUP DATABASE football_simulator TO Disk('backups', 'backup_$(date +%Y%m%d)')"

# Restore
docker exec $(docker ps -q -f name=clickhouse) \
  clickhouse-client --query "RESTORE DATABASE football_simulator FROM Disk('backups', 'backup_20240115')"
```

### Prometheus Data

Prometheus data is stored in the `prometheus-data` volume:

```bash
# Create volume backup
docker run --rm -v prometheus-data:/data -v /backup:/backup alpine \
  tar czf /backup/prometheus-$(date +%Y%m%d).tar.gz -C /data .
```

## Troubleshooting

### SSL Certificate Issues

```bash
# Check Traefik logs
docker service logs football-simulator_traefik

# Verify DNS
dig api.your-domain.com

# Check certificate
echo | openssl s_client -connect api.your-domain.com:443 2>/dev/null | openssl x509 -noout -dates
```

### Service Won't Start

```bash
# Check service status
docker service ps football-simulator_go-api --no-trunc

# View task errors
docker service inspect football-simulator_go-api

# Force update
docker service update --force football-simulator_go-api
```

### High Memory Usage

```bash
# Check resource usage
docker stats

# View ClickHouse memory
docker exec $(docker ps -q -f name=clickhouse) \
  clickhouse-client --query "SELECT * FROM system.metrics WHERE metric LIKE '%Memory%'"

# Increase instance size
# Update CloudFormation InstanceType parameter
```

### Kafka Issues

```bash
# Check Kafka logs
docker service logs football-simulator_kafka

# List topics
docker exec $(docker ps -q -f name=kafka) \
  kafka-topics.sh --bootstrap-server localhost:9092 --list

# Describe consumer groups
docker exec $(docker ps -q -f name=kafka) \
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

## Cost Estimate

| Resource | Monthly Cost (us-east-1) |
|----------|--------------------------|
| r5.xlarge (on-demand) | ~$182 |
| 300GB gp3 EBS | ~$36 |
| Elastic IP | ~$3.65 |
| Data transfer (50GB) | ~$4.50 |
| **Total** | **~$226/month** |

**Cost Optimization:**
- Reserved instances: Save up to 72%
- Spot instances: Save up to 90% (not recommended for production)
- Right-size instance based on actual usage

## Security Checklist

- [ ] Restrict SSH access (SSHAccessCIDR parameter)
- [ ] Rotate API keys regularly
- [ ] Enable ECR image scanning
- [ ] Review IAM permissions
- [ ] Set up CloudWatch alarms
- [ ] Enable VPC Flow Logs
- [ ] Configure backup retention
- [ ] Test disaster recovery
