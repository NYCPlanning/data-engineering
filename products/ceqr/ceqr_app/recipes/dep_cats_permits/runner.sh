#!/bin/bash
set -e
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output
    # csv, shapefile, and README from the version ingest archived ($ARCHIVE_VERSION)
    python3 build.py

    (
        cd output

        # Write VERSION info
        echo "$VERSION" > version.txt

        # Convert README.md to README.pdf
        mdpdf --output ReadMe_DEPCATS.pdf README.md
    )
    Upload_data_operations $NAME $VERSION
    Upload_data_operations $NAME staging
    rm -rf output
)
