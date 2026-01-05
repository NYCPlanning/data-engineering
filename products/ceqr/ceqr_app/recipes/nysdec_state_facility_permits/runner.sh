#!/bin/bash
set -e
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output
    
    python3 build.py
    (
        cd output
        
        # Export to CSV
        psql $EDM_DATA --set ON_ERROR_STOP=1 -c "\COPY (
            SELECT * FROM $NAME.\"$VERSION\"
        ) TO stdout DELIMITER ',' CSV HEADER;" > $NAME.csv
        psql $EDM_DATA --set ON_ERROR_STOP=1 -c "\COPY (
            SELECT * FROM $NAME.geo_rejects
        ) TO stdout DELIMITER ',' CSV HEADER;" > geo_rejects.csv

        # Export to ShapeFile
        SHP_export $EDM_DATA $NAME.$VERSION POINT $NAME

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )

    Upload $NAME $VERSION
    Upload $NAME latest
    rm -rf output
)
