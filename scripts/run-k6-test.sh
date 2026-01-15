#!/bin/bash
# =============================================================================
# k6 Load Test Runner
# =============================================================================
# Runs k6 load tests and streams logs
#
# Usage:
#   ./scripts/run-k6-test.sh              # Run spike test
#   ./scripts/run-k6-test.sh --watch      # Run and watch logs
#   ./scripts/run-k6-test.sh --stop       # Stop running test
# =============================================================================

set -e

STACK_NAME="${STACK_NAME:-football-simulator}"
SERVICE_NAME="${STACK_NAME}_k6"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --watch    Run test and follow logs"
    echo "  --stop     Stop running test"
    echo "  --status   Show test status"
    echo "  --help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0              # Start spike test"
    echo "  $0 --watch      # Start and follow logs"
    echo "  $0 --stop       # Stop the test"
}

start_test() {
    echo -e "${BLUE}Starting k6 load test...${NC}"

    # Check if service exists
    if ! docker service ls --format '{{.Name}}' | grep -q "^${SERVICE_NAME}$"; then
        echo -e "${RED}Error: k6 service not found. Run ./scripts/deploy.sh first${NC}"
        exit 1
    fi

    # Scale up k6 service to start the test
    docker service scale "${SERVICE_NAME}=1" --detach=false

    echo ""
    echo -e "${GREEN}k6 test started!${NC}"
    echo ""
    echo "View logs:    docker service logs -f ${SERVICE_NAME}"
    echo "View Grafana: https://grafana.\${DOMAIN}"
    echo ""
}

stop_test() {
    echo -e "${YELLOW}Stopping k6 load test...${NC}"
    docker service scale "${SERVICE_NAME}=0" --detach=false
    echo -e "${GREEN}Test stopped${NC}"
}

show_status() {
    echo -e "${BLUE}k6 Service Status:${NC}"
    docker service ps "${SERVICE_NAME}" --no-trunc 2>/dev/null || echo "No running tasks"
}

# Parse arguments
case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --stop)
        stop_test
        exit 0
        ;;
    --status)
        show_status
        exit 0
        ;;
    --watch)
        start_test
        echo -e "${BLUE}Following k6 logs (Ctrl+C to stop watching)...${NC}"
        echo ""
        docker service logs -f "${SERVICE_NAME}"
        ;;
    "")
        start_test
        ;;
    *)
        echo -e "${RED}Unknown option: $1${NC}"
        show_help
        exit 1
        ;;
esac
