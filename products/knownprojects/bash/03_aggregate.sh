#!/bin/bash
source ../../bash/utils.sh
set_error_traps

## Do preprocessing for aggregations
echo "Create ZAP Project Many BBLs table"
run_sql_file sql/aggregate/create_zap_projects.sql

echo "Preprocess column names to standardize"
run_sql_file sql/aggregate/preprocessing.sql

## Do aggregations
echo "Create the aggregate tables..."
dbt build --select tag:aggregate_general tag:aggregate_sca tag:cpp \
    --warn-error-options '{"error": ["NoNodesForSelectionCriteria"]}'

echo "All aggregations are complete"
