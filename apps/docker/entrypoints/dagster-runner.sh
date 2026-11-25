#!/bin/bash
# Dagster code server for the single data-engineering code location (apps/dagster/definitions.py).
# The webserver/daemon app (entrypoints/dagster.sh with DAGSTER_CODE_SERVER_HOST) connects to it on
# port 4000. With the default run launcher, runs execute as subprocesses of this server, so they
# need the same instance storage (DAGSTER_PG_URL) as the webserver.
set -e

source "$(dirname "$0")/dagster-common.sh"

exec dagster code-server start -h 0.0.0.0 -p 4000 -m definitions -a defs \
    --location-name data-engineering
