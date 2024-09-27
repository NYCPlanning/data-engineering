#!/bin/bash
source ./pluto_build/bash/config.sh   # assuming this script is run from pluto/ dir
set_error_traps

echo "Setup dbt"
dbt deps
dbt debug

echo "Build seed tables"
dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh

echo "Test source tables"
dbt test --select "source:*"

echo "Build staging tables"
dbt build --select staging

echo "Build intermediate QAQC tables"
dbt run --select qaqc   # need to use 'dbt run' because otherwise it fails if downstream test fails

echo "🔥 Run DE aka important tests 🔥"
dbt test --select tag:de_check
