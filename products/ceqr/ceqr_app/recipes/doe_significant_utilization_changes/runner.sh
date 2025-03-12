#!/bin/bash
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR

    endpoint=https://edm-recipes.nyc3.digitaloceanspaces.com
    V=$(curl $endpoint/datasets/doe_pepmeetingurls/latest/config.json | jq -r '.dataset.version')
    curl $endpoint/datasets/doe_pepmeetingurls/$V/doe_pepmeetingurls.sql | psql $EDM_DATA

    psql -q $RECIPE_ENGINE -f build.sql| 
    psql $EDM_DATA -v NAME=$NAME -v VERSION=$VERSION -f create.sql

    mkdir -p output && 
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