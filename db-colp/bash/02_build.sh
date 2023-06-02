#!/bin/bash
source bash/config.sh

psql $BUILD_ENGINE -f sql/load_modifications.sql  

psql $BUILD_ENGINE -f sql/geo_inputs.sql

docker run --rm\
    --network host\
    -v `pwd`:/home/colp_build/\
    -w /home/colp_build\
    -u $(id -u ${USER}):$(id -g ${USER}) \
    -e BUILD_ENGINE=$BUILD_ENGINE\
    nycplanning/docker-geosupport:latest bash -c "python3 -m python.geocode;
                                                  python3 -m python.geo_qaqc"

psql $BUILD_ENGINE -f sql/_procedures.sql
psql $BUILD_ENGINE -f sql/clean_parcelname.sql
psql $BUILD_ENGINE -f sql/create_colp.sql


psql $BUILD_ENGINE -1 -c "CALL apply_correction('_colp', 'modifications');"

echo "Generate output tables"
psql $BUILD_ENGINE -f sql/export_colp.sql
