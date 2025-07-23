#!/bin/bash
source ../../bash/utils.sh
set_error_traps

run_sql_file sql/ccp_commitments.sql
run_sql_file sql/ccp_budgets.sql
run_sql_file sql/ccp_projects.sql
run_sql_file sql/budget_data.sql

# create the table
echo 'Creating Attributes Table'
run_sql_file sql/attributes.sql -v build_schema=${BUILD_ENGINE_SCHEMA}

# categorize the projects
echo 'Categorizing projects'
run_sql_file sql/attributes_typecategory.sql

## Geometries
### Old values can be overwritten later
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

# update cpdb_dcpattributes with geoms from sprints
echo 'Updating geometries from old sprints'
run_sql_command "
    UPDATE cpdb_dcpattributes a 
    SET geomsource = b.geomsource, geom = b.geom 
    FROM sprints as b 
    WHERE a.maprojid = b.maprojid
    AND b.geom IS NOT NULL;" 

# These manual geometries may overwrite old sprints. That's good.
run_sql_command "
      UPDATE cpdb_dcpattributes a
      SET geomsource = 'DCP_geojson', 
          geom = ST_SetSRID(ST_GeomFromText(ST_AsText(b.geom)), 4326)
      FROM dcp_json b
      WHERE b.maprojid = a.maprojid
      AND b.geom IS NOT NULL;
      "
echo 'Loading geometries from id->bin->footprint mapping'
run_sql_file sql/attributes_id_bin_map.sql

# Create 2020 manual geometry table

echo 'Creating 2020 manual geometry table'
cat 'data/edcgeoms_2021-01-08.csv' |
run_sql_command "
    DROP TABLE IF EXISTS manual_geoms_2020;
    CREATE TABLE manual_geoms_2020 (
        cartoid text, 
        maprojid text,
        project_discription text,
        footprint_project_id text,
        footprint_project_geomsource text,
        geom geometry);
    COPY manual_geoms_2020 FROM STDIN DELIMITER ',' CSV HEADER;
    UPDATE manual_geoms_2020 a
        SET footprint_project_geomsource = 'EP 2020'
        WHERE a.footprint_project_geomsource ~* 'AD Sprint';
    SELECT UpdateGeometrySRID('manual_geoms_2020','geom',4326);"

run_sql_command "
    UPDATE cpdb_dcpattributes a 
    SET geomsource = b.footprint_project_geomsource, geom = b.geom 
    FROM manual_geoms_2020 as b 
    WHERE a.maprojid = b.maprojid
    AND b.geom IS NOT NULL;" 


# agency geometries
# These should not be overwritten unless by ddc so ddc is last.
# These may overwrite old geometries from above.

# dot
echo 'Adding DOT geometries'
run_sql_file sql/attributes_dot.sql

# dpr
echo 'Adding DPR geometries by FMS ID'
run_sql_file sql/attributes_dpr_fmsid.sql

# edc
echo 'Adding EDC geometries'
run_sql_file sql/attributes_edc.sql

# ddc
echo 'Adding DDC geometries'
run_sql_file sql/attributes_ddc.sql

# agency verified Summer 2017
# create geoms for agency mapped projects
echo 'Creating geometries for agency verified data - Summer 2017'
run_sql_file sql/attributes_agencyverified_geoms.sql

# geocode agencyverified
python3 python/attributes_geom_agencyverified_geocode.py

# These may overwrite any geometry from above
echo 'Adding agency verified geometries'
run_sql_file sql/attributes_agencyverified.sql

# String matching should never overwrite a geometry from above

# dpr -- fuzzy string on park id
echo 'Adding DPR geometries based on string matching for park id'
run_sql_file sql/attributes_dpr_string_id.sql

# dpr -- fuzzy string on park name
echo 'Adding DPR geometries based on string matching for park name'
run_sql_file sql/attributes_dpr_string_name.sql

echo
echo "################################"
echo "# attributes relational tables #"
echo "################################"
echo

# facilities relational table
echo 'Creating facilities relational table based on fuzzy string matching'
run_sql_file sql/attributes_maprojid_facilities.sql

# and add geometries from FabDB based on that match above
run_sql_file sql/attributes_facilities.sql

# 05_geocode_maprojid_parkid
python3 python/attributes_maprojid_parkid.py

echo 'Creating maprojid --> parkid relational table'
run_sql_file sql/attributes_maprojid_parkid.sql

echo
echo "###########################"
echo "# final geometry clean-up #"
echo "###########################"
echo

# geometry cleaning -- lines to polygons and all geoms to multi
echo 'Cleaning geometries: lines to polygons and geoms to multi'
run_sql_file sql/attributes_geomclean.sql

# remove faulty geometries	
echo 'Removing bad geometries'
run_sql_file sql/attributes_badgeoms.sql	

# create final table
echo 'Creating projects tables'
run_sql_file sql/projects.sql
