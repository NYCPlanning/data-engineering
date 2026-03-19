#!/bin/bash
source bash/config.sh

# Load seeds via dbt
dbt seed --select usetype_mappings
run_sql_file sql/load_modifications.sql
run_sql_file sql/geo_inputs.sql
