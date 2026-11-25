#!/bin/bash
# Dagster webserver + daemon in one container.
#
# With DAGSTER_CODE_SERVER_HOST set, user code lives in a separate code server
# (entrypoints/dagster-runner.sh) and this container loads none of it. Without it, the webserver
# and daemon each spawn their own code servers from workspace.yaml and runs execute here.
set -e

source "$(dirname "$0")/dagster-common.sh"

if [ -n "${DAGSTER_CODE_SERVER_HOST}" ]; then
    export DAGSTER_CODE_SERVER_PORT="${DAGSTER_CODE_SERVER_PORT:-4000}"
    workspace=workspace.grpc.yaml
    echo "Loading code from ${DAGSTER_CODE_SERVER_HOST}:${DAGSTER_CODE_SERVER_PORT}" >&2
else
    workspace=workspace.yaml
fi

dagster-daemon run -w "${workspace}" &
exec dagster-webserver -h 0.0.0.0 -p 3000 -w "${workspace}"
