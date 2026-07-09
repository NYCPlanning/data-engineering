#!/bin/bash
# Dagster entrypoint: clone repo, install dcpy, start supervisord

set -e

echo "[$(date -Iseconds)] Dagster container starting..."

# Initial repo clone/update
/app/docker/scripts/update-repo.sh

# Install dcpy with all app dependencies
echo "[$(date -Iseconds)] Installing dcpy[all-apps]..."
cd /app/repos/data-engineering
uv pip install --system -e ".[all-apps]"

# Create supervisor log directory
mkdir -p /var/log/supervisor

# Set working directory for dagster to the cloned repo's dagster app
cd /app/repos/data-engineering/apps/dagster

# Start supervisord (will manage dagster-webserver, dagster-daemon, and reload-loop)
echo "[$(date -Iseconds)] Starting supervisord..."
exec supervisord -c /app/docker/supervisord/dagster.conf
