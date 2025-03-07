#!/bin/bash
source $(pwd)/bin/config.sh
BASEDIR=$(dirname $0)
NAME=$(basename $BASEDIR)
VERSION=$DATE

(
    cd $BASEDIR

    mkdir -p output && 
    (
        cd output

        echo "a,b,c,d" > _test.csv
        zip _test.csv.zip _test.csv
        echo "a,b,c,d" > README.md
        echo "a,b,c,d" > README.pdf
        echo "data dictionary" > data_dictionary.xlsx

        # Write VERSION info
        echo "$VERSION" > version.txt
        
    )
    Upload $NAME $VERSION
    Upload $NAME latest
    rm -rf output
)