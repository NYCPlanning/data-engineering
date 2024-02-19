#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Setup dbt"
dbt deps
dbt debug

echo "Test source data"
dbt test --select "source:*"

echo "Build all tables"
dbt build --full-refresh
