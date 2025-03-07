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
    psql $EDM_DATA --set ON_ERROR_STOP=1 -v NAME=$NAME -v VERSION=$VERSION -f create.sql

    (
        cd output

        # Export to CSV
        psql $EDM_DATA --set ON_ERROR_STOP=1 -c "\COPY (
            SELECT * FROM $NAME.\"$VERSION\"
        ) TO stdout DELIMITER ',' CSV HEADER;" > $NAME.csv

        # Export to ShapeFile
        SHP_export $EDM_DATA $NAME.$VERSION POINT $NAME

        # Write VERSION info
        echo "$VERSION" > version.txt

        # Convert README.md to README.pdf
        mdpdf --output ReadMe_DEPCATS.pdf README.md
    )
    Upload_data_operations $NAME $VERSION
    Upload_data_operations $NAME staging
    rm -rf output
)
