#!/bin/bash
# Dagster webserver + daemon in one container (POC).
set -e

cd /app/data-engineering/apps/dagster

# The ingest code location reads templates from TEMPLATE_DIR, which defaults to a path relative to
# the repo root - not where we run from.
export TEMPLATE_DIR="${TEMPLATE_DIR:-/app/data-engineering/ingest_templates}"

# Postgres instance storage when configured; otherwise Dagster's default SQLite under DAGSTER_HOME,
# so the app still starts with no secrets.
if [ -n "${DAGSTER_PG_URL}" ]; then
    cp dagster.yaml "${DAGSTER_HOME}/dagster.yaml"
else
    rm -f "${DAGSTER_HOME}/dagster.yaml"
    echo "DAGSTER_PG_URL not set: using SQLite storage in ${DAGSTER_HOME} (not persisted)" >&2
fi

dagster-daemon run &
exec dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml --path-prefix /dag
