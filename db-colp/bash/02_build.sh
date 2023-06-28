#!/bin/bash
source ../bash/utils.sh
set_env ../.env

run_sql_file sql/load_modifications.sql  

run_sql_file sql/geo_inputs.sql

python3 -m python.geocode
python3 -m python.geo_qaqc"

run_sql_file sql/_procedures.sql
run_sql_file sql/clean_parcelname.sql
run_sql_file sql/create_colp.sql


run_sql_command "CALL apply_correction('_colp', 'modifications');"

echo "Generate output tables"
run_sql_file sql/export_colp.sql
