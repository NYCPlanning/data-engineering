#!/bin/bash
source config.sh

echo "CBBR Version $VERSION : 03 Spatial"
# TODO delete tables that are created by this stage
echo "Geocode with geosupport image  ..."
docker run --rm \
    -v $(pwd):/home/db-cbbr \
    -w /home/db-cbbr \
    --env BUILD_ENGINE \
    --network="host" \
    nycplanning/docker-geosupport:latest bash -c "python3 -m library.geocode"

echo "Assign DoiTT geometries to geocoded data ..."
run_sql sql/assign_geoms.sql

# ## Skipping for dev of initial FY2024 build
# # echo "Assign geometries from manual shapefiles ..."
# # run_sql sql/spatial_manualshp.sql

echo "Assign geometries from parks data ..."
run_sql sql/spatial_dpr_string_name.sql

echo "Assign geometries from facilities data ..."
run_sql sql/spatial_facilities.sql
 
echo "Overwriting geometries with manual mapping..."
run_sql sql/apply_corrections.sql

echo "Running spatial_geomclean ..."
run_sql sql/spatial_geomclean.sql

echo "Running geo_rejects ..."
run_sql sql/geo_rejects.sql

echo "Done!"
