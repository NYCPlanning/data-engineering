#!/bin/bash

set -e

cd "$(dirname "$0")"

if [ ! -f "../../admin/run_environment/constraints.txt" ]; then
    echo "Error: admin/run_environment/constraints.txt not found"
    exit 1
fi

if [ ! -f "../../admin/run_environment/requirements.txt" ]; then
    echo "Error: admin/run_environment/requirements.txt not found"
    exit 1
fi

if [ ! -f "../../pyproject.toml" ]; then
    exit 1
fi

docker build -t nycplanning/dcpy-notebook-server:latest -f Dockerfile ../..
