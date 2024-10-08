#!/bin/bash
source ../../bash/utils.sh
set_error_traps

## Do preprocessing for aggregations
echo "Create ZAP Project Many BBLs table"
run_sql_file sql/aggregate/create_zap_projects.sql

echo "Preprocess column names to standardize"
run_sql_file sql/aggregate/preprocessing.sql

## Do SCA aggregations
echo "Create the SCA aggregation tables..."

echo "Build Elementary School Zones aggregate table"
run_sql_file sql/aggregate/sca/boundaries_es_zone.sql

echo "Build School Districts aggregate table"
run_sql_file sql/aggregate/sca/boundaries_school_districts.sql

echo "Build School Subdistricts aggregate table"
run_sql_file sql/aggregate/sca/boundaries_school_subdistricts.sql
echo "SCA sggregations are complete"

## Do general aggregations
echo "Create the general aggregate tables..."

echo "Build Census Tract aggregate table"
run_sql_file sql/aggregate/longform_ct_output.sql
dbt build --select future_units_by_ct

echo "Build NTA aggregate table"
run_sql_file sql/aggregate/longform_nta_output.sql
dbt build --select future_units_by_nta

echo "Build CDTA aggregate table"
run_sql_file sql/aggregate/longform_cdta_output.sql
dbt build --select future_units_by_cdta

echo "Build Community District aggregate table"
run_sql_file sql/aggregate/longform_cd_output.sql
dbt build --select future_units_by_cd

echo "General aggregations are complete"

echo "All aggregations are complete"
