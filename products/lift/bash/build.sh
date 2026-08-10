#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# lift is a DuckDB-only build (no BUILD_ENGINE_* vars) - point dbt at the
# DuckDB file the loader produced for this recipe/version.
export DUCKDB_PATH=$(python3 -m dcpy lifecycle builds build path --duckdb --recipe recipe)
echo "Using DuckDB file: ${DUCKDB_PATH}"

echo "Setup dbt"
dbt deps
dbt debug

echo "Test source tables"
dbt test --select "source:*"

echo "Build staging tables"
dbt build --select staging

echo "Build intermediate tables"
dbt build --select intermediate

echo "Build product tables"
dbt build --select product
