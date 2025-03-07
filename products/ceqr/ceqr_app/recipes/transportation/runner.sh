#!/bin/bash
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR
    mkdir -p output

    (
        cd output

        # Export to CSV
        cp ../../_data/transportation_* .

        # Write VERSION info
        echo "$VERSION" > version.txt

        # # Convert README.md to README.pdf
        # docker run --rm\
        #     -v "`pwd`:/data" \
        #     --user `id -u`:`id -g` \
        #     pandoc/latex README.md -o README.pdf
        
    )
    
    Upload $NAME $VERSION
    Upload $NAME latest
)