#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# The cpdb_dcpattributes/ccp_*/budget_data build chain that used to live here has been ported to
# dbt (models/intermediate, models/product). What's left is prep for the two pieces still built
# outside dbt and declared as sources (models/sources_legacy.yml): the sprints table, and the
# agency-verified geocode chain, whose Python step depends on this SQL gap-fill having run first
# to know which rows still need geocoding.

# create old sprints table
echo 'Creating old sprints table'
run_sql_command "
    DROP TABLE IF EXISTS sprints;
    CREATE TABLE sprints (
        maprojid text,
        bbl text,
        bin text,
        geomsource text,
        geom geometry)"

run_sql_file data/sprints.sql

# agency verified Summer 2017
# create geoms for agency mapped projects
echo 'Creating geometries for agency verified data - Summer 2017'
run_sql_file sql/attributes_agencyverified_geoms.sql

# geocode agencyverified
python3 python/attributes_geom_agencyverified_geocode.py

# write geocode results back into dcp_cpdb_agencyverified's geom column (sql/analysis reads it
# directly)
echo 'Adding agency verified geometries'
run_sql_file sql/attributes_agencyverified.sql
