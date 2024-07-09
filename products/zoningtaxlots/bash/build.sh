#!/bin/bash
source ../../bash/utils.sh
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

echo "Build intermediate tables"
dbt build --select intermediate

echo "Build product and qaqc tables"
dbt build --select product

