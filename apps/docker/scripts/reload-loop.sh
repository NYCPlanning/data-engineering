#!/bin/bash
# Periodically updates the git repo and triggers app reload

set -e

RELOAD_INTERVAL="${RELOAD_INTERVAL:-60}"  # seconds between checks
RELOAD_COMMAND="${RELOAD_COMMAND:-}"      # command to run after update (app-specific)

echo "[$(date -Iseconds)] Starting reload loop (interval: ${RELOAD_INTERVAL}s)"

while true; do
    # Update the repo
    /app/docker/scripts/update-repo.sh

    # If a reload command is specified, run it
    if [ -n "$RELOAD_COMMAND" ]; then
        echo "[$(date -Iseconds)] Running reload command: $RELOAD_COMMAND"
        eval "$RELOAD_COMMAND" || echo "[$(date -Iseconds)] WARNING: Reload command failed (exit code: $?)"
    fi

    # Wait before next check
    sleep "$RELOAD_INTERVAL"
done
