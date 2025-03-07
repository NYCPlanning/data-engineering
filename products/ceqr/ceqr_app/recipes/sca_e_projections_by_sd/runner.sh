#!/bin/bash
set -e
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output

    psql $RECIPE_ENGINE --set ON_ERROR_STOP=1 --file build.sql
    psql $EDM_DATA --set NAME=$NAME --set VERSION=$VERSION --set ON_ERROR_STOP=1 --file create.sql

    (
        cd output

        # Export to CSV
        psql $EDM_DATA -c "\COPY (
            SELECT * FROM $NAME.\"$VERSION\"
        ) TO stdout DELIMITER ',' CSV HEADER;" > $NAME.csv

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )
    
    Upload $NAME $VERSION
    Upload $NAME latest
)