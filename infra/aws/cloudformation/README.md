# AWS CloudFormation Deployment

This directory contains CloudFormation templates for deploying the Fanfinity Football Event Streaming Infrastructure to AWS EC2 with Docker Swarm.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         AWS Cloud                                │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │                         VPC                                │  │
│  │  ┌─────────────────────────────────────────────────────┐  │  │
│  │  │              Public Subnet (10.0.1.0/24)            │  │  │
│  │  │                                                      │  │  │
│  │  │  ┌──────────────────────────────────────────────┐   │  │  │
│  │  │  │           EC2 Instance (r5.xlarge)           │   │  │  │
│  │  │  │                                              │   │  │  │
│  │  │  │  Docker Swarm Manager                        │   │  │  │
│  │  │  │  ├── Traefik (ports 80, 443)                │   │  │  │
│  │  │  │  ├── go-api (x3 replicas)                   │   │  │  │
│  │  │  │  ├── go-consumer (x2 replicas)              │   │  │  │
│  │  │  │  ├── Kafka                                   │   │  │  │
│  │  │  │  ├── ClickHouse                             │   │  │  │
│  │  │  │  ├── Prometheus                             │   │  │  │
│  │  │  │  └── Grafana                                │   │  │  │
│  │  │  │                                              │   │  │  │
│  │  │  │  300GB gp3 EBS (6000 IOPS)                  │   │  │  │
│  │  │  └──────────────────────────────────────────────┘   │  │  │
│  │  │                        │                             │  │  │
│  │  │                   Elastic IP                         │  │  │
│  │  └────────────────────────┼─────────────────────────────┘  │  │
│  └───────────────────────────┼────────────────────────────────┘  │
│                              │                                    │
│  ┌───────────────────────────┼────────────────────────────────┐  │
│  │  ECR Repositories         │                                │  │
│  │  ├── go-api              DNS                               │  │
│  │  └── go-consumer          │                                │  │
│  └───────────────────────────┼────────────────────────────────┘  │
└──────────────────────────────┼───────────────────────────────────┘
                               │
                         ┌─────┴─────┐
                         │  Internet │
                         └───────────┘
```

## Prerequisites

1. **AWS CLI** installed and configured with appropriate permissions
2. **EC2 Key Pair** created in your target region
3. **Domain name** configured (for SSL certificates)

## Quick Start

### 1. Create EC2 Key Pair (if you don't have one)

```bash
# Create key pair and save private key
aws ec2 create-key-pair \
  --key-name fanfinity-key \
  --query 'KeyMaterial' \
  --output text > fanfinity-key.pem

chmod 400 fanfinity-key.pem
```

### 2. Update Parameters

Edit `parameters/dev.json` and replace placeholder values:

```json
{
  "ParameterKey": "Domain",
  "ParameterValue": "your-domain.com"      // Your domain
},
{
  "ParameterKey": "LetsEncryptEmail",
  "ParameterValue": "admin@your-domain.com" // Your email
},
{
  "ParameterKey": "KeyPairName",
  "ParameterValue": "fanfinity-key"         // Your key pair name
}
```

### 3. Deploy Stack

```bash
# Validate template
aws cloudformation validate-template \
  --template-body file://single-node-swarm.yaml

# Create stack
aws cloudformation create-stack \
  --stack-name fanfinity-dev \
  --template-body file://single-node-swarm.yaml \
  --parameters file://parameters/dev.json \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1

# Wait for completion (takes ~10-15 minutes)
aws cloudformation wait stack-create-complete \
  --stack-name fanfinity-dev \
  --region us-east-1

# Get outputs
aws cloudformation describe-stacks \
  --stack-name fanfinity-dev \
  --query 'Stacks[0].Outputs' \
  --output table
```

### 4. Configure DNS

Create DNS A records pointing to the Elastic IP (from stack outputs):

| Record | Type | Value |
|--------|------|-------|
| `api.your-domain.com` | A | `<Elastic IP>` |
| `grafana.your-domain.com` | A | `<Elastic IP>` |
| `prometheus.your-domain.com` | A | `<Elastic IP>` |
| `traefik.your-domain.com` | A | `<Elastic IP>` |

### 5. Configure GitHub Secrets

Add these secrets to your GitHub repository (Settings > Secrets > Actions):

| Secret | Value (from stack outputs) |
|--------|---------------------------|
| `AWS_REGION` | `us-east-1` |
| `AWS_ACCOUNT_ID` | Your AWS account ID |
| `AWS_ROLE_ARN` | `GitHubActionsRoleArn` output |
| `ECR_REGISTRY` | `ECRRegistry` output |
| `STACK_NAME` | `fanfinity-dev` |
| `EC2_INSTANCE_ID` | `InstanceId` output |

### 6. Deploy Application

SSH into the instance and deploy:

```bash
# SSH into instance
ssh -i fanfinity-key.pem ec2-user@<elastic-ip>

# Clone repository
cd /opt/fanfinity
git clone https://github.com/YOUR_USERNAME/fanfinity-infrastructure.git app
cd app

# Copy generated environment file
cp /opt/fanfinity/.env .

# Deploy all services
./scripts/deploy.sh

# View generated credentials
cat /opt/fanfinity/credentials.txt
```

## CI/CD Pipeline

The GitHub Actions workflow (`.github/workflows/deploy.yml`) automates:

### Triggers

| Event | Action |
|-------|--------|
| Push to `main` | Build with `latest` tag, deploy |
| Tag `v*.*.*` | Build with version tag, deploy, create release |
| Pull request | Build only (no push/deploy) |
| Manual dispatch | Build and optionally deploy with custom version |

### Workflow Steps

1. **Build**: Docker images built and pushed to ECR
2. **Deploy**: Services updated via SSM command
3. **Release**: GitHub release created (for version tags)

### Creating a Release

```bash
# Create and push a version tag
git tag v1.0.0
git push origin v1.0.0
```

This triggers the full pipeline: build → push → deploy → release.

## Stack Resources

| Resource | Type | Purpose |
|----------|------|---------|
| VPC | Networking | Isolated network |
| PublicSubnet | Networking | EC2 placement |
| InternetGateway | Networking | Internet access |
| SwarmSecurityGroup | Security | Firewall rules |
| EC2Role | IAM | Instance permissions |
| GitHubActionsRole | IAM | CI/CD permissions |
| GitHubOIDCProvider | IAM | Secure GitHub auth |
| ElasticIP | Networking | Static public IP |
| SwarmInstance | Compute | Docker Swarm node |
| GoApiRepository | ECR | API Docker images |
| GoConsumerRepository | ECR | Consumer Docker images |

## Ports

| Port | Service | Access |
|------|---------|--------|
| 22 | SSH | Restricted (SSHAccessCIDR) |
| 80 | HTTP | Public (Let's Encrypt) |
| 443 | HTTPS | Public (Traefik) |
| 8080 | Traefik Dashboard | Restricted |
| 9090 | Prometheus | Restricted |
| 3000 | Grafana | Restricted |

## Instance Types

| Type | vCPU | RAM | Use Case |
|------|------|-----|----------|
| t3.xlarge | 4 | 16GB | Minimum for testing |
| t3.2xlarge | 8 | 32GB | Budget option |
| **r5.xlarge** | 4 | 32GB | **Recommended** |
| r5.2xlarge | 8 | 64GB | High headroom |
| r6i.xlarge | 4 | 32GB | Newer generation |
| r6i.2xlarge | 8 | 64GB | Newer generation |

## Useful Commands

```bash
# View stack status
aws cloudformation describe-stacks --stack-name fanfinity-dev

# View stack events (for debugging)
aws cloudformation describe-stack-events --stack-name fanfinity-dev

# Update stack
aws cloudformation update-stack \
  --stack-name fanfinity-dev \
  --template-body file://single-node-swarm.yaml \
  --parameters file://parameters/dev.json \
  --capabilities CAPABILITY_NAMED_IAM

# Delete stack
aws cloudformation delete-stack --stack-name fanfinity-dev

# Connect via SSM (no SSH key needed)
aws ssm start-session --target <instance-id>

# View Docker services on EC2
ssh -i key.pem ec2-user@<ip> "docker service ls"

# View service logs
ssh -i key.pem ec2-user@<ip> "docker service logs fanfinity_go-api"
```

## Troubleshooting

### Stack creation fails

Check CloudWatch Logs:
- `/fanfinity-dev/user-data` - Instance setup logs
- `/fanfinity-dev/docker` - Docker logs

Or view user-data output on instance:
```bash
cat /var/log/user-data.log
```

### Services not starting

```bash
# Check service status
docker service ls
docker service ps fanfinity_go-api

# View service logs
docker service logs fanfinity_go-api --tail 100
```

### SSL certificate issues

Ensure DNS records are properly configured and propagated:
```bash
dig api.your-domain.com
```

Traefik needs DNS to resolve to the Elastic IP for Let's Encrypt validation.

## Cost Estimate (us-east-1)

| Resource | Monthly Cost |
|----------|-------------|
| r5.xlarge (on-demand) | ~$182 |
| 300GB gp3 (6000 IOPS) | ~$36 |
| Elastic IP | ~$3.65 |
| Data transfer (50GB) | ~$4.50 |
| **Total** | **~$226/month** |

Tips to reduce costs:
- Use Reserved Instances (up to 72% savings)
- Use Spot Instances for dev/test
- Reduce EBS IOPS to baseline 3000 (saves ~$10/month)
- Use t3.xlarge for testing (~$121/month)

## Security Considerations

1. **Restrict SSH access**: Set `SSHAccessCIDR` to your IP instead of `0.0.0.0/0`
2. **Use SSM**: Connect via Systems Manager instead of SSH
3. **Rotate credentials**: Regenerate API keys and passwords periodically
4. **Enable MFA**: Add MFA to AWS account
5. **Review security groups**: Audit open ports regularly
