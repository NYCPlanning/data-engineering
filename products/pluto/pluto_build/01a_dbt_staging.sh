#!/bin/bash
source ./bash/config.sh
set_error_traps

echo "Loading DBT seeds (lookup tables)..."
cd ..
dbt seed --profiles-dir . --target ${DBT_TARGET:-dev}
if [ $? -ne 0 ]; then
    echo "ERROR: DBT seeds failed to load"
    exit 1
fi
echo "✓ DBT seeds loaded successfully"

echo "Materializing DBT staging models..."
dbt run --select staging --profiles-dir . --target ${DBT_TARGET:-dev}
if [ $? -ne 0 ]; then
    echo "ERROR: DBT staging models failed to materialize"
    exit 1
fi
echo "✓ DBT staging models materialized successfully"

cd pluto_build
