#!/bin/bash
# Start docker-compose for local development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"

cd "$APP_DIR"

echo ""
echo "Starting services in local development mode..."
echo ""

# Start with local overrides
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build

echo ""
echo "Services started!"
echo ""
echo "Access your services at:"
echo "  - QA App:  http://localhost:8080/qaqc"
echo "  - Dagster: http://localhost:8080/dag"
echo ""
echo "Login with:"
echo "  - Username: detools"
echo "  - Password: (from your environment)"
echo ""
echo "View logs:"
echo "  docker-compose logs -f [service-name]"
echo ""
echo "Stop services:"
echo "  docker-compose down"
