#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Load source data
rm -rf data
mkdir -p data

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
python3 -m python.extractors dcp_planneradded
python3 -m python.extractors dcp_knownprojects

# Load corrections tables
run_sql_file sql/create_corrections.sql

echo
echo "data loading complate"
echo
