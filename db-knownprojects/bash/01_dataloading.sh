#!/bin/bash
source bash/config.sh
max_bg_procs 5

# Load source data
for f in $(ls data/processed)
do 
    psql $BUILD_ENGINE -f data/processed/$f &
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
psql $BUILD_ENGINE -f sql/create_corrections.sql

echo
echo "data loading complate"
echo
