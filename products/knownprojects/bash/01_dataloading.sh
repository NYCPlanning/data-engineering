#!/bin/bash
source ../../bash/utils.sh
set_error_traps
max_bg_procs 5

if [ -n "${BUILD_ENGINE_SCHEMA}" ]; then
    echo "Dropping and creating build schema '$BUILD_ENGINE_SCHEMA'"
    run_sql_command "DROP SCHEMA IF EXISTS ${BUILD_ENGINE_SCHEMA} CASCADE;"
    run_sql_command "VACUUM (ANALYZE);"
    run_sql_command "CREATE SCHEMA ${BUILD_ENGINE_SCHEMA};"
fi

# Load source data
# TODO create data folder and populate it with data from S3 rather than a copy of the data repo
rm -rf data
mkdir -p data

# download data/raw
# download data/corrections
python3 -m python.download

# extract data/raw -> data/processed
python3 -m python.extractors esd_projects
python3 -m python.extractors edc_projects
python3 -m python.extractors edc_dcp_inputs
python3 -m python.extractors dcp_n_study
python3 -m python.extractors dcp_n_study_future
python3 -m python.extractors dcp_n_study_projected
python3 -m python.extractors hpd_rfp
python3 -m python.extractors hpd_pc
python3 -m python.extractors dcp_planneradded

for f in $(ls data/processed)
do 
    run_sql_file data/processed/$f
done

create_source_data_table

# Load ZAP tables
import_recipe dcp_projects &
import_recipe dcp_projectactions &
import_recipe dcp_projectbbls &
import_recipe dcp_dcpprojectteams &

# Load other tables
import_recipe dcp_mappluto_wi &
import_recipe dcp_boroboundaries & 
import_recipe dcp_housing &
import_recipe dcp_zoningmapamendments &

# Add SCA Geometry Aggregate Tables
import_recipe doe_eszones &
import_recipe doe_school_subdistricts & 
import_recipe dcp_school_districts
wait

# Load corrections tables
run_sql_file sql/create_corrections.sql

echo
echo "data loading complate"
echo
