#!/bin/bash
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    echo "loading into table atypical_roadways.\"$VERSION\""
    psql -q $RECIPE_ENGINE -f build.sql| 
    psql $EDM_DATA -v NAME=$NAME -v VERSION=$VERSION -f create.sql

    mkdir -p output && 
    (
        cd output

        # Export to CSV
        psql $EDM_DATA -c "\COPY (
            SELECT * FROM atypical_roadways.\"$VERSION\"
        ) TO stdout DELIMITER ',' CSV HEADER;" > atypical_roadways.csv

        # Export to ShapeFile
        SHP_export $EDM_DATA atypical_roadways.$VERSION LINESTRING atypical_roadways

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )
    Upload $NAME $VERSION
    Upload $NAME latest
)