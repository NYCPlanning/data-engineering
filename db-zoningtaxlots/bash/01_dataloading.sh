#!/bin/bash
source ../bash_utils/config.sh
source bash/config.sh
set_env ../.env
set_error_traps

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

# Generate source_data_versions table
run_sql_command "
  DROP TABLE IF EXISTS source_data_versions;
  SELECT 
    datasource as schema_name, 
    version as v
  INTO source_data_versions
  FROM versions;
" -1
