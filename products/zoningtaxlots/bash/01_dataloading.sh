#!/bin/bash
source bash/config.sh

echo "Dropping and creating build schema '$BUILD_ENGINE_SCHEMA'"
# create build schema
run_sql_command \
    "
    DROP SCHEMA IF EXISTS ${BUILD_ENGINE_SCHEMA} CASCADE;
    CREATE SCHEMA ${BUILD_ENGINE_SCHEMA};
    "

create_source_data_table

# Import Data
import_recipe dcp_commercialoverlay &
import_recipe dcp_limitedheight &
import_recipe dcp_specialpurpose &
import_recipe dcp_specialpurposesubdistricts &
import_recipe dcp_zoningmapamendments &
import_recipe dof_dtm &
import_recipe dcp_zoningdistricts &
import_recipe dcp_zoningmapindex &
wait

rm -rf .library
