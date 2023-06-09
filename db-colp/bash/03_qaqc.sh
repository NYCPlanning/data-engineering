#!/bin/bash
source ../bash_utils/config.sh

echo "Running QAQC"

run_sql_file sql/geo_qaqc.sql
run_sql_file sql/qc_geospatial_check.sql 
run_sql_file sql/colp_qaqc.sql