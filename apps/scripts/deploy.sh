#!/bin/bash
# Production deployment script for Digital Ocean droplet
# Run with: op run --env-file=env.1pw -- ./scripts/deploy.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"

cd "$APP_DIR"

echo "=== Starting deployment ==="

# Pull latest images
echo "Pulling latest Docker images..."
docker compose pull qa-streamlit

# Build dagster image
echo "Building Dagster image..."
docker compose build dagster

# Restart services with updated images
echo "Restarting services..."
docker compose up -d --force-recreate qa-streamlit dagster

# Ensure nginx is running
echo "Ensuring nginx is running..."
docker compose up -d nginx certbot

# Clean up old images
echo "Cleaning up old Docker images..."
docker image prune -f

echo "=== Deployment complete ==="
echo ""
echo "Services status:"
docker compose ps
