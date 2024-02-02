#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Create ZAP Project Many BBLs table"
run_sql_file sql/aggregate/create_zap_projects.sql

echo "Preprocess column names to standardize"
run_sql_file sql/aggregate/preprocessing.sql

## Do SCA aggregation
echo "Create the longfrom SCA Aggregate Tables..."

# Aggregate KPDB projects to Elementary School Zones 
echo "Build Elementary School Zones Aggregate Table"
run_sql_file sql/aggregate/sca/boundaries_es_zone.sql

# Aggregate KPDB projects to School District Zones
echo "Build School Districts aggregate table"
run_sql_file sql/aggregate/sca/boundaries_school_districts.sql

# Aggregate KPDB projects to School Subdistrict Zones
echo "Build School Subdistricts aggregate tables"
run_sql_file sql/aggregate/sca/boundaries_school_subdistricts.sql

echo "SCA sggregations are complete"

## Do other aggregation
# Aggregate KPDB projects to Community Districts
echo "Build Community Districts aggregate tables"
run_sql_file sql/aggregate/longform_ct_output.sql
run_sql_file sql/aggregate/longform_nta_output.sql
run_sql_file sql/aggregate/longform_cdta_output.sql
run_sql_file sql/aggregate/longform_cd_output.sql

echo "Other aggregations are complete"

run_sql_command "ALTER TABLE _kpdb RENAME COLUMN geometry TO geom;"

echo "All aggregations are complete"
