#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Load source data
rm -rf data
mkdir -p data

# These pinned versions predate the ingest migration, so their geometry columns
# aren't named geom. The dcp_housing parquet also declares no CRS, so it loads with
# SRID 0 even though the coordinates are lon/lat. dcp_mappluto_wi loads from the
# pg_dump because bbl is null in every row of the 25v1.1 parquet.
run_sql_command "ALTER TABLE dcp_mappluto_wi RENAME COLUMN wkb_geometry TO geom;"
run_sql_command "ALTER TABLE dcp_housing RENAME COLUMN geometry TO geom;"
run_sql_command "ALTER TABLE dcp_housing ALTER COLUMN geom TYPE geometry USING ST_SetSRID(geom, 4326);"

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
python3 -m python.extractors dcp_hudsonsquare

# Load corrections tables
run_sql_file sql/create_corrections.sql

echo
echo "data loading complate"
echo
