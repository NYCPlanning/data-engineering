#!/bin/bash

set -e

cd "$(dirname "$0")"

# Create temporary build context
BUILD_DIR=$(mktemp -d)
trap "rm -rf $BUILD_DIR" EXIT

echo "Creating optimized build context with hardlinks in $BUILD_DIR"

# Hardlink only required files/directories (fast, no copying)
mkdir -p "$BUILD_DIR/admin/run_environment"
mkdir -p "$BUILD_DIR/apps/notebook-server"
mkdir -p "$BUILD_DIR/dcpy"
mkdir -p "$BUILD_DIR/notebooks"

# Hardlink individual files
ln ../../admin/run_environment/constraints.txt "$BUILD_DIR/admin/run_environment/"
ln ../../admin/run_environment/requirements.txt "$BUILD_DIR/admin/run_environment/"
ln ../../pyproject.toml "$BUILD_DIR/"

# Hardlink directories recursively
cp -al ../../dcpy/* "$BUILD_DIR/dcpy/" 2>/dev/null || true
cp -al ../../notebooks/* "$BUILD_DIR/notebooks/" 2>/dev/null || echo "No notebooks directory found"
cp -al ./* "$BUILD_DIR/apps/notebook-server/"

# Build from the optimized context
echo "Building Docker image with optimized context..."
docker build -t nycplanning/dcpy-notebook-server:latest -f Dockerfile "$BUILD_DIR"

echo "Build complete. Context was $(du -sh $BUILD_DIR | cut -f1) (hardlinked vs $(du -sh ../.. | cut -f1) for full repo)."
