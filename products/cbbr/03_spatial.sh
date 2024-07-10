#!/bin/bash
source config.sh
set_error_traps

echo "CBBR Version ${VERSION} : 03 Spatial"
# TODO delete tables that are created by this stage
echo "Geocode ..."
python3 -m geocode.geosupport

echo "Assign DoiTT geometries to geocoded data ..."
run_sql_file sql/assign_geoms.sql

echo "Assign geometries from parks data ..."
run_sql_file sql/spatial_dpr_string_name.sql

echo "Assign geometries from facilities data ..."
run_sql_file sql/spatial_facilities.sql

echo "Assign geometries from manual shapefiles ..."
run_sql_file sql/spatial_manual_map.sql

echo "Running spatial_geomclean ..."
run_sql_file sql/spatial_geomclean.sql

echo "Running geo_rejects ..."
run_sql_file sql/geo_rejects.sql

echo "Done!"
