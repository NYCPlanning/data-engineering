#!/bin/bash
source ../../bash/utils.sh
set_error_traps
max_bg_procs 5

if [ -n "${BUILD_ENGINE_SCHEMA}" ]; then
    echo "Dropping and creating build schema '$BUILD_ENGINE_SCHEMA'"
    run_sql_command "DROP SCHEMA IF EXISTS ${BUILD_ENGINE_SCHEMA} CASCADE;"
    # run_sql_command "VACUUM (FULL);"
    run_sql_command "CREATE SCHEMA ${BUILD_ENGINE_SCHEMA};"
fi

# Load source data
rm -rf data
mkdir -p data

create_source_data_table

# download data/raw
# download data/corrections
python3 -m python.download

# extract data/raw -> SQL DB
python3 -m python.extractors esd_projects
python3 -m python.extractors edc_projects
python3 -m python.extractors edc_dcp_inputs
python3 -m python.extractors dcp_n_study
python3 -m python.extractors dcp_n_study_future
python3 -m python.extractors dcp_n_study_projected
python3 -m python.extractors hpd_rfp
python3 -m python.extractors hpd_pc
python3 -m python.extractors dcp_planneradded
python3 -m python.extractors dcp_knownprojects

# Load corrections tables
run_sql_file sql/create_corrections.sql

# * Versions pinned for Capital Plannings team's mid-schedule enhancements release
geosupport_version=23a
# Load ZAP tables
import_recipe dcp_projects
import_recipe dcp_projectactions
import_recipe dcp_projectbbls
import_recipe dcp_dcpprojectteams

# Load other tables
import_recipe dcp_mappluto_wi
import_recipe dcp_boroboundaries
import_recipe dcp_housing
import_recipe dcp_zoningmapamendments

# Load SCA Geometry Aggregate Tables
import_recipe doe_eszones
import_recipe doe_school_subdistricts
import_recipe dcp_school_districts

# Load geographic boundaries Aggregate Tables
import_recipe dcp_ct2020_wi
import_recipe dcp_nta2020
import_recipe dcp_cdta2020
import_recipe dcp_cdboundaries_wi

echo
echo "data loading complate"
echo
