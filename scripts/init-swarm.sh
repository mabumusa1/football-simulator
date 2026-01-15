#!/bin/bash
# =============================================================================
# Docker Swarm Initialization Script
# =============================================================================
# Run this script on the MANAGER node to initialize the swarm
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Docker Swarm Initialization${NC}"
echo -e "${GREEN}========================================${NC}"

# -----------------------------------------------------------------------------
# Check if Docker is installed
# -----------------------------------------------------------------------------
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    echo "Install Docker first:"
    echo "  sudo yum install docker -y"
    echo "  sudo systemctl enable docker"
    echo "  sudo systemctl start docker"
    exit 1
fi

# -----------------------------------------------------------------------------
# Check if user is in docker group
# -----------------------------------------------------------------------------
if ! groups | grep -q docker; then
    echo -e "${YELLOW}Warning: Current user is not in docker group${NC}"
    echo "You may need to run: sudo usermod -aG docker \$USER"
    echo "Then log out and back in"
fi

# -----------------------------------------------------------------------------
# Check if already in a swarm
# -----------------------------------------------------------------------------
SWARM_STATUS=$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null)

if [ "$SWARM_STATUS" = "active" ]; then
    echo -e "${YELLOW}This node is already part of a swarm${NC}"
    echo ""
    echo "Current swarm info:"
    docker node ls
    echo ""
    echo "To get the worker join token:"
    echo "  docker swarm join-token worker"
    echo ""
    echo "To leave the swarm and reinitialize:"
    echo "  docker swarm leave --force"
    exit 0
fi

# -----------------------------------------------------------------------------
# Get the advertise address
# -----------------------------------------------------------------------------
# Try to get the public IP (for AWS EC2)
PUBLIC_IP=$(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || true)

# Fall back to private IP
if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP=$(hostname -I | awk '{print $1}')
fi

echo -e "${GREEN}Detected IP address: ${PUBLIC_IP}${NC}"
echo ""

# Allow override via argument
if [ -n "$1" ]; then
    PUBLIC_IP="$1"
    echo -e "${YELLOW}Using provided IP: ${PUBLIC_IP}${NC}"
fi

# -----------------------------------------------------------------------------
# Initialize the swarm
# -----------------------------------------------------------------------------
echo -e "${GREEN}Initializing Docker Swarm...${NC}"
docker swarm init --advertise-addr "$PUBLIC_IP"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Swarm initialized successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# -----------------------------------------------------------------------------
# Display worker join command
# -----------------------------------------------------------------------------
echo -e "${YELLOW}To add WORKER nodes, run this command on each worker:${NC}"
echo ""
docker swarm join-token worker | grep "docker swarm join"
echo ""

# -----------------------------------------------------------------------------
# Display manager join command
# -----------------------------------------------------------------------------
echo -e "${YELLOW}To add MANAGER nodes (for HA), run this command:${NC}"
echo ""
docker swarm join-token manager | grep "docker swarm join"
echo ""

# -----------------------------------------------------------------------------
# Next steps
# -----------------------------------------------------------------------------
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Next Steps:${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "1. Join worker nodes using the command above"
echo ""
echo "2. Label nodes for placement constraints:"
echo "   docker node update --label-add clickhouse=true <worker-node-name>"
echo "   docker node update --label-add kafka=true <worker-node-name>"
echo ""
echo "3. Verify nodes:"
echo "   docker node ls"
echo ""
echo "4. Deploy the stack:"
echo "   ./scripts/deploy.sh"
echo ""
