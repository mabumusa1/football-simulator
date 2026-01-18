#!/bin/bash
# =============================================================================
# Football Simulator Stack Deployment Script
# =============================================================================
# Merges service files and deploys to Docker Swarm
#
# Usage:
#   ./scripts/deploy.sh              # Deploy all services
#   ./scripts/deploy.sh traefik      # Deploy only Traefik
#   ./scripts/deploy.sh kafka        # Deploy only Kafka
#   ./scripts/deploy.sh --list       # List available services
#   ./scripts/deploy.sh --generate   # Generate merged docker-compose.yml
# =============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICES_DIR="$PROJECT_DIR/infra/compose/prod"
STACK_NAME="${STACK_NAME:-football-simulator}"

# Available services (order matters for dependencies)
SERVICES=(
    "traefik"
    "kafka"
    "clickhouse"
    "go-api"
    "go-consumer"
    "monitoring"
)

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

show_help() {
    echo "Usage: $0 [OPTIONS] [SERVICE...]"
    echo ""
    echo "Options:"
    echo "  --list       List available services"
    echo "  --generate   Generate merged docker-compose.yml without deploying"
    echo "  --help       Show this help message"
    echo ""
    echo "Services:"
    for svc in "${SERVICES[@]}"; do
        echo "  $svc"
    done
    echo ""
    echo "Examples:"
    echo "  $0                    # Deploy all services"
    echo "  $0 traefik            # Deploy only Traefik"
    echo "  $0 traefik kafka      # Deploy Traefik and Kafka"
    echo "  $0 --generate         # Generate merged file only"
}

get_service_file() {
    local svc="$1"
    echo "$SERVICES_DIR/$svc.yml"
}

list_services() {
    echo -e "${BLUE}Available services:${NC}"
    for svc in "${SERVICES[@]}"; do
        local svc_file=$(get_service_file "$svc")
        if [ -f "$svc_file" ]; then
            echo -e "  ${GREEN}✓${NC} $svc"
        else
            echo -e "  ${RED}✗${NC} $svc (file not found)"
        fi
    done
}

check_prerequisites() {
    echo -e "${BLUE}Checking prerequisites...${NC}"

    # Check swarm mode
    if [ "$(docker info --format '{{.Swarm.LocalNodeState}}')" != "active" ]; then
        echo -e "${RED}Error: Docker Swarm not active${NC}"
        echo "Run: ./scripts/init-swarm.sh"
        exit 1
    fi

    # Check manager node
    if [ "$(docker info --format '{{.Swarm.ControlAvailable}}')" != "true" ]; then
        echo -e "${RED}Error: Not a swarm manager${NC}"
        exit 1
    fi

    # Check .env file
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        echo -e "${YELLOW}Warning: .env not found, copying from .env.example${NC}"
        if [ -f "$PROJECT_DIR/.env.example" ]; then
            cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
            echo -e "${YELLOW}Please edit .env with your values${NC}"
            exit 1
        fi
    fi

    echo -e "${GREEN}Prerequisites OK${NC}"
}

build_compose_command() {
    local services=("$@")
    local cmd="docker compose"

    # Include env file
    cmd="$cmd --env-file $PROJECT_DIR/.env"

    # Always include base
    cmd="$cmd -f $PROJECT_DIR/infra/compose/base.yml"

    # Add requested services
    if [ ${#services[@]} -eq 0 ]; then
        # All services
        for svc in "${SERVICES[@]}"; do
            local svc_file=$(get_service_file "$svc")
            if [ -f "$svc_file" ]; then
                cmd="$cmd -f $svc_file"
            fi
        done
    else
        # Specific services
        for svc in "${services[@]}"; do
            local svc_file=$(get_service_file "$svc")
            if [ -f "$svc_file" ]; then
                cmd="$cmd -f $svc_file"
            else
                echo -e "${RED}Error: Service file not found: $svc.yml${NC}"
                exit 1
            fi
        done
    fi

    echo "$cmd"
}

generate_merged() {
    local services=("$@")
    local output="$PROJECT_DIR/docker-compose.generated.yml"

    echo -e "${BLUE}Generating merged docker-compose.yml...${NC}"

    local cmd=$(build_compose_command "${services[@]}")
    $cmd config > "$output"

    echo -e "${GREEN}Generated: $output${NC}"
    echo ""
    echo "To deploy manually:"
    echo "  docker stack deploy -c $output $STACK_NAME"
}

deploy_stack() {
    local services=("$@")

    echo -e "${BLUE}Deploying stack: ${STACK_NAME}${NC}"

    # Build the merged config
    local cmd=$(build_compose_command "${services[@]}")

    # Deploy using docker stack (reads from merged config)
    # Fixes for docker compose v5 -> docker stack compatibility:
    # 1. Convert published port strings to integers
    # 2. Remove 'name:' property not supported by docker stack
    $cmd config | sed 's/published: "\([0-9]*\)"/published: \1/g' | sed '/^name:/d' | docker stack deploy -c - "$STACK_NAME"

    echo ""
    echo -e "${GREEN}Deployment initiated!${NC}"
    echo ""

    # Wait and show status
    sleep 5
    echo -e "${BLUE}Service Status:${NC}"
    docker stack services "$STACK_NAME"
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

cd "$PROJECT_DIR"

# Parse arguments
SELECTED_SERVICES=()
ACTION="deploy"

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --list|-l)
            list_services
            exit 0
            ;;
        --generate|-g)
            ACTION="generate"
            shift
            ;;
        -*)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
        *)
            SELECTED_SERVICES+=("$1")
            shift
            ;;
    esac
done

# Execute action
case $ACTION in
    generate)
        generate_merged "${SELECTED_SERVICES[@]}"
        ;;
    deploy)
        check_prerequisites
        deploy_stack "${SELECTED_SERVICES[@]}"
        ;;
esac
