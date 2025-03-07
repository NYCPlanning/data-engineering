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
    docker run --rm\
        -v $(pwd)/../:/recipes\
        -w /recipes/$NAME\
        --user $UID\
        -e EDM_DATA=$EDM_DATA\
        nycplanning/docker-geosupport:latest python3 build.py | 
    psql $EDM_DATA --set ON_ERROR_STOP=1 -v NAME=$NAME -v VERSION=$VERSION -f create.sql

    echo "Running export ..."
    (
        cd output
        
        # Export to CSV
        psql $EDM_DATA -c "\COPY (
            SELECT * FROM $NAME.\"$VERSION\"
        ) TO stdout DELIMITER ',' CSV HEADER;" > $NAME.csv

        # Export to ShapeFile
        SHP_export $EDM_DATA $NAME.$VERSION POINT $NAME

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )

    echo "Running upload ..."
    Upload $NAME $VERSION
    Upload $NAME latest

    echo "Done!"
)