---
layout: default
title: Deployment
nav_order: 5
---

# Deployment Guide

Deploy Fanfinity Infrastructure to production on AWS using Docker Swarm.

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
  --key-name fanfinity-key \
  --query 'KeyMaterial' \
  --output text > fanfinity-key.pem

# Secure the key
chmod 400 fanfinity-key.pem
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
    "ParameterValue": "fanfinity.example.com"
  },
  {
    "ParameterKey": "LetsEncryptEmail",
    "ParameterValue": "admin@example.com"
  },
  {
    "ParameterKey": "KeyPairName",
    "ParameterValue": "fanfinity-key"
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
  --stack-name fanfinity-prod \
  --template-body file://infra/aws/cloudformation/single-node-swarm.yaml \
  --parameters file://infra/aws/cloudformation/parameters/prod.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1

# Wait for completion (10-15 minutes)
aws cloudformation wait stack-create-complete \
  --stack-name fanfinity-prod \
  --region us-east-1

# Get outputs
aws cloudformation describe-stacks \
  --stack-name fanfinity-prod \
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
api.fanfinity.example.com      → <Elastic IP>
grafana.fanfinity.example.com  → <Elastic IP>
prometheus.fanfinity.example.com → <Elastic IP>
traefik.fanfinity.example.com  → <Elastic IP>
```

Verify DNS propagation:
```bash
dig api.fanfinity.example.com
```

## Step 5: Configure GitHub Secrets

Add these secrets to your GitHub repository (Settings → Secrets → Actions):

| Secret | Value | Source |
|--------|-------|--------|
| `AWS_REGION` | us-east-1 | Your region |
| `AWS_ACCOUNT_ID` | 123456789012 | CloudFormation output |
| `AWS_ROLE_ARN` | arn:aws:iam::... | GitHubActionsRoleArn output |
| `ECR_REGISTRY` | 123456789012.dkr.ecr... | ECRRegistry output |
| `STACK_NAME` | fanfinity-prod | Your stack name |
| `EC2_INSTANCE_ID` | i-0123456789abcdef0 | InstanceId output |
| `SONAR_TOKEN` | sqa_... | From SonarCloud |

## Step 6: Initial Deployment

### Option A: GitHub Actions (Recommended)

Push to main branch or create a version tag:

```bash
# Push to main (deploys with "latest" tag)
git push origin main

# Or create a version tag
git tag v1.0.0
git push origin v1.0.0
```

### Option B: Manual Deployment

SSH into the instance and deploy:

```bash
# SSH into instance
ssh -i fanfinity-key.pem ec2-user@<elastic-ip>

# Navigate to app directory
cd /opt/fanfinity

# Clone repository (if not auto-cloned)
git clone https://github.com/your-org/fanfinity-infrastructure.git app
cd app

# Copy environment file
cp /opt/fanfinity/.env .

# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <ecr-registry>

# Deploy all services
./scripts/deploy.sh
```

## Step 7: Verify Deployment

```bash
# Check Swarm services
docker service ls

# Expected output:
# ID     NAME                    MODE         REPLICAS   IMAGE
# xxx    fanfinity_traefik       replicated   1/1        traefik:3.6.7
# xxx    fanfinity_kafka         replicated   1/1        apache/kafka:4.1.1
# xxx    fanfinity_clickhouse    replicated   1/1        clickhouse/clickhouse-server:25...
# xxx    fanfinity_go-api        replicated   3/3        <ecr>/go-api:latest
# xxx    fanfinity_go-consumer   replicated   2/2        <ecr>/go-consumer:latest
# xxx    fanfinity_prometheus    replicated   1/1        prom/prometheus:v3.9.1
# xxx    fanfinity_grafana       replicated   1/1        grafana/grafana:12.4.0

# Test API
curl https://api.fanfinity.example.com/health

# Test with API key
curl https://api.fanfinity.example.com/api/events \
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
cat /opt/fanfinity/credentials.txt
```

## CI/CD Pipeline

The GitHub Actions pipeline (`.github/workflows/deploy.yml`) automates:

1. **Build** - Docker images for go-api and go-consumer
2. **Push** - Images to ECR with version tags
3. **Deploy** - Via SSM SendCommand to EC2
4. **Release** - GitHub release for version tags

**Triggers:**

| Trigger | Version Tag | Deploy |
|---------|-------------|--------|
| Push to main | `latest` | Yes |
| Tag v*.*.* | Tag name | Yes + Release |
| Pull request | `pr-<number>` | Build only |
| Manual dispatch | Custom | Optional |

## Scaling

### Scale API Service

```bash
# Scale up
docker service scale fanfinity_go-api=5

# Scale down
docker service scale fanfinity_go-api=2
```

### Scale Consumer Service

```bash
docker service scale fanfinity_go-consumer=4
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
  fanfinity_go-api

# Update go-consumer
docker service update \
  --image <ecr>/go-consumer:v1.1.0 \
  fanfinity_go-consumer
```

### Full Stack Redeploy

```bash
cd /opt/fanfinity/app
git pull
./scripts/deploy.sh
```

## Monitoring

### View Service Logs

```bash
# API logs
docker service logs fanfinity_go-api --tail 100 -f

# Consumer logs
docker service logs fanfinity_go-consumer --tail 100

# All services
docker service logs fanfinity_kafka
docker service logs fanfinity_clickhouse
docker service logs fanfinity_traefik
```

### Grafana Dashboards

1. Open https://grafana.your-domain.com
2. Navigate to Dashboards
3. View pre-configured dashboards:
   - k6 Load Test
   - System Metrics
   - API Performance

### CloudWatch Logs

Logs are also available in CloudWatch:
- `/fanfinity-prod/user-data` - EC2 setup logs
- `/fanfinity-prod/docker` - Docker/service logs

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
docker service logs fanfinity_traefik

# Verify DNS
dig api.your-domain.com

# Check certificate
echo | openssl s_client -connect api.your-domain.com:443 2>/dev/null | openssl x509 -noout -dates
```

### Service Won't Start

```bash
# Check service status
docker service ps fanfinity_go-api --no-trunc

# View task errors
docker service inspect fanfinity_go-api

# Force update
docker service update --force fanfinity_go-api
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
docker service logs fanfinity_kafka

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
