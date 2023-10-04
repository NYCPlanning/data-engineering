#!/bin/bash
source bash/config.sh

# TODO mimic template.load
# Reference tables
echo "Dropping and creating build schema '$BUILD_ENGINE_SCHEMA'"
# create build schema
run_sql_command \
    "
    DROP SCHEMA IF EXISTS ${BUILD_ENGINE_SCHEMA} CASCADE;
    CREATE SCHEMA ${BUILD_ENGINE_SCHEMA};
    "
create_source_data_table
run_sql_file "sql/_create.sql"

# Spatial boundaries
import_recipe dcp_boroboundaries_wi &
import_recipe dcp_stateassemblydistricts &
import_recipe dcp_ct2020 &
import_recipe dcp_congressionaldistricts &
import_recipe dcp_cdboundaries &
import_recipe dcp_statesenatedistricts &
import_recipe dcp_municipalcourtdistricts &
import_recipe dcp_school_districts &
import_recipe dcp_trafficanalysiszones &
import_recipe dcp_councildistricts &
import_recipe nypd_policeprecincts &
import_recipe fdny_firecompanies &

# Building and lot-level info
import_recipe dcp_mappluto_wi &
import_recipe dcp_facilities &
import_recipe doitt_buildingfootprints &

# Projects
import_recipe cpdb_capital_spending &
import_recipe fisa_capitalcommitments &
import_recipe dot_projects_intersections &
import_recipe dot_projects_streets &
import_recipe dot_projects_bridges &
import_recipe dpr_capitalprojects &
import_recipe dpr_parksproperties &
import_recipe edc_capitalprojects_ferry &
import_recipe edc_capitalprojects &
import_recipe ddc_capitalprojects_infrastructure &
import_recipe ddc_capitalprojects_publicbuildings &
wait

echo "fixing dot_bridges"
run_sql_command "ALTER TABLE dot_projects_bridges RENAME COLUMN fmsid TO fms_id;"
python3 python/dot_bridges.py

echo "fixing doitt_buildingfootprints"
run_sql_command "ALTER TABLE doitt_buildingfootprints RENAME TO doitt_buildingfootprints_source;"
python3 python/doitt_buildingfootprints.py
run_sql_command "DROP TABLE IF EXISTS doitt_buildingfootprints_source;"


echo "renaming cpdb_capital_spending to capital_spending"
run_sql_command "DROP TABLE IF EXISTS capital_spending;"
run_sql_command "ALTER TABLE cpdb_capital_spending RENAME TO capital_spending;"
