#!/bin/bash
# Updates the data-engineering repo from git (clone on first run, fetch+reset thereafter)

set -e

REPO_URL="${REPO_URL:-https://github.com/NYCPlanning/data-engineering.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_DIR="${REPO_DIR:-/app/repos/data-engineering}"

# If repo doesn't exist, clone it
if [ ! -d "$REPO_DIR/.git" ]; then
    echo "[$(date -Iseconds)] Cloning $REPO_URL (branch: $REPO_BRANCH) to $REPO_DIR..."
    mkdir -p "$(dirname "$REPO_DIR")"
    git clone --depth 1 --branch "$REPO_BRANCH" "$REPO_URL" "$REPO_DIR"
    echo "[$(date -Iseconds)] Clone completed"
else
    # Repo exists, fetch and reset to latest
    echo "[$(date -Iseconds)] Updating $REPO_DIR to latest $REPO_BRANCH..."
    cd "$REPO_DIR"

    # Fetch latest changes
    git fetch origin "$REPO_BRANCH" --depth 1

    # Reset to latest (handles force pushes gracefully)
    git reset --hard "origin/$REPO_BRANCH"

    echo "[$(date -Iseconds)] Update completed (now at $(git rev-parse --short HEAD))"
fi
