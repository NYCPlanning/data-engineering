#!/bin/bash
source ./pluto_build/bash/config.sh   # assuming this script is run from pluto/ dir
set_error_traps

echo "Setup dbt"
dbt deps
dbt debug

echo "Build seed tables"
dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh

echo "Test source tables"
dbt test --select "source:*" --exclude tag:de_check

echo "Build staging tables"
dbt build --select staging --exclude tag:de_check

echo "Build intermediate QAQC tables"
dbt build --select qaqc --exclude tag:de_check

echo "🔥 Run DE aka important tests 🔥"
dbt test --select tag:de_check,tag:$VERSION_TYPE    # this will only run tests that have both tags, not just one of them
