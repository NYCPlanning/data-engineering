#!/bin/bash
# Dagster webserver + daemon in one container (POC).
set -e

cd /app/data-engineering/apps/dagster
dagster-daemon run &
exec dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml --path-prefix /dag
