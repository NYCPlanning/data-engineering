#!/bin/bash
# Create a postgres database container
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
WORKDIR="$(pwd)/$NAME"
CONTAINER_NAME="cbbr_cook_container"
CONTAINER_WORKDIR="/cook_container_home/$NAME"
VERSION=$DATE

source $WORKDIR/config.sh

echo "CBBR Version $VERSION : 01 Data Loading"
echo "Load data into the container ..."

## DEPRICATED USE OF COOK DOCKER IMAGE
# docker run --rm \
#     --name $CONTAINER_NAME \
#     --volume $WORKDIR:$CONTAINER_WORKDIR \
#     --workdir $CONTAINER_WORKDIR \
#     --env-file .env \
#     nycplanning/cook:latest python3 python/dataloading.py
# nycplanning/cook:latest bash -c "
# set -e
# python3 python/dataloading.py;
# # python3 python/aggregate_geoms.py;
# # pip3 install -r python/requirements.txt;
# # python3 python/manual_geoms.py
# "
# psql $BUILD_ENGINE -f sql/preprocessing.sql

## REPLACES python3 python/dataloading.py in cook image
import_public cbbr_submissions
import_public dpr_parksproperties $DPR_PARKS_VERSION
import_public dcp_facilities $DCP_FACILITIES_VERSION
import_public doitt_buildingfootprints $DOITT_FOOTPRINTS_VERSION # last version with a valid sql archive in in edm-recipes

echo "Create Manually Mapped Corrections table..."
run_sql sql/create_corrections.sql

## Skipping aggregate_geoms for dev of initial FY2024 build
# echo "Aggregate geometries ..."
# ## REPLACES python3 python/aggregate_geoms.py in cook image
# python3 $WORKDIR/python/aggregate_geoms.py

## Skipping manual_geoms for dev of initial FY2024 build
# ## REPLACES python3 python/manual_geoms.py in cook image
# python3 $WORKDIR/python/manual_geoms.py

## Skipping import of FY2021-FY2022 lookup table for dev of initial FY2024 build
# echo "Load FY2021 to FY2022 lookup data ..."
# cat $WORKDIR/data/cbbr_fy22_to_fy21_uniqueids.csv |
#     psql $BUILD_ENGINE --set ON_ERROR_STOP=1 --command "
#     DROP TABLE IF EXISTS fy21_fy22_lookup;
#     CREATE TABLE fy21_fy22_lookup (
#         fy22_unique_id text,
#         fy21_unique_id text
#     );
#     COPY fy21_fy22_lookup FROM STDIN DELIMITER ',' CSV HEADER;
# "

echo "Done!"
