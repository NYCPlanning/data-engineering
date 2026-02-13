#!/bin/bash
source ./bash/config.sh
set_error_traps

echo "Materializing DBT staging models..."

# Go to pluto product directory (parent of pluto_build)
cd ..

# Run DBT staging models
# Use BUILD_ENGINE_SCHEMA environment variable if set, otherwise default to public
echo "Running: dbt run --select staging"
dbt run --select staging --profiles-dir . --target ${DBT_TARGET:-dev}

if [ $? -ne 0 ]; then
    echo "ERROR: DBT staging models failed to materialize"
    exit 1
fi

echo "✓ DBT staging models materialized successfully"

# Return to pluto_build directory
cd pluto_build
