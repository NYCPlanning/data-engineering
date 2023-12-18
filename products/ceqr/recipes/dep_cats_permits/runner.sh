#!/bin/bash
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output

    docker run --rm\
        -e EDM_DATA=$EDM_DATA\
        -v $(pwd)/../:/recipes\
        -w /recipes/$NAME\
        --user $UID\
        nycplanning/docker-geosupport:latest python3 build.py | 
    psql $EDM_DATA -v NAME=$NAME -v VERSION=$VERSION -f create.sql

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

        # Convert README.md to README.pdf
        docker run --rm\
            -v "`pwd`:/data" \
            --user `id -u`:`id -g` \
            pandoc/latex README.md -o ReadMe_DEPCATS.pdf
    )
    Upload $NAME $VERSION
    Upload $NAME latest
    rm -rf output
)
