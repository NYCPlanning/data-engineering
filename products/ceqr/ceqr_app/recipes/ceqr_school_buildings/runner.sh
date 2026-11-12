#!/bin/bash
set -e
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output

    echo "Running build.sql ..."
    psql -q $RECIPE_ENGINE --set ON_ERROR_STOP=1 -f build.sql

    echo "Running build.py and create.sql ..."
    python3 build.py | 
    psql $EDM_DATA --set ON_ERROR_STOP=1 -v NAME=$NAME -v VERSION=$VERSION -f create.sql

    echo "Running export ..."
    (
        cd output
        
        # Export to CSV
        psql $EDM_DATA -c "\COPY (
            SELECT * FROM $NAME
        ) TO stdout DELIMITER ',' CSV HEADER;" > $NAME.csv

        # Export to ShapeFile
        SHP_export $EDM_DATA $BUILD_ENGINE_SCHEMA.$NAME POINT $NAME

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )

    echo "Running upload ..."
    Upload $NAME $VERSION
    Upload $NAME latest

    echo "Done!"
)