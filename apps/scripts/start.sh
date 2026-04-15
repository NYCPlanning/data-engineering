#!/bin/bash
# Start services (production)
# Run with: op run --env-file=env.1pw -- ./scripts/start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"

cd "$APP_DIR"

echo "=== Starting services ==="

# Check if running with 1Password CLI
echo "Starting docker compose..."
docker compose up -d

echo "=== Services started ==="
docker compose ps
