#!/bin/bash
source ../../bash/utils.sh
source bash/config.sh
set_error_traps
max_bg_procs 5

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


# Load ZAP tables
import_private dcp_projects &
import_private dcp_projectactions &
import_private dcp_projectbbls &
import_private dcp_dcpprojectteams &

# Load other tables
import_public dcp_mappluto_wi &
import_public dcp_boroboundaries & 
import_public dcp_housing &
import_public dcp_zoningmapamendments &

# Add SCA Geometry Aggregate Tables
import_public doe_eszones &
import_public doe_school_subdistricts & 
import_public dcp_school_districts
wait

# Load corrections tables
run_sql_file sql/create_corrections.sql

echo
echo "data loading complate"
echo
