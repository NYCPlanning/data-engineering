#!/bin/bash
function upload_to_bigquery {
    local dataset=${1:-dcp_mappluto}
    local schema=${2:-$dataset}
    FILEPATH=$dataset/$VERSION/$dataset.csv
    echo https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$FILEPATH
    curl -O https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$FILEPATH
    gsutil cp $dataset.csv gs://edm-temporary/$FILEPATH
    rm $dataset.csv
    location=US
    dataset=$dataset
    tablename=$dataset.$(tr [A-Z] [a-z] <<< "$VERSION")
    echo $tablename
    bq show $dataset || bq mk --location=$location --dataset $dataset
    bq show $tablename || bq mk $tablename
    bq load \
        --location=$location\
        --source_format=CSV\
        --quote '"' \
        --skip_leading_rows 1\
        --replace\
        --allow_quoted_newlines\
        $tablename \
        gs://edm-temporary/$FILEPATH \
        ../schemas/$schema.json
}

register 'bq' 'publish' 'publish to bigquery' upload_to_bigquery

function upload_to_bigquery_historical {
    local dataset=${1:-dcp_mappluto}
    local version=${2:-$dataset}
    local schemafile=dcp_mappluto_$version.json
    FILEPATH=$dataset/$version/$dataset.csv
    echo https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$FILEPATH
    curl -O https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$FILEPATH
    gsutil cp $dataset.csv gs://edm-temporary/$FILEPATH
    rm $dataset.csv
    location=US
    dataset=$dataset
    tablename=$dataset.$(tr [A-Z] [a-z] <<< "$version")
    echo $tablename
    bq show $dataset || bq mk --location=$location --dataset $dataset
    bq show $tablename || bq mk $tablename
    bq load \
        --location=$location\
        --source_format=CSV\
        --quote '"' \
        --skip_leading_rows 1\
        --replace\
        --allow_quoted_newlines\
        $tablename \
        gs://edm-temporary/$FILEPATH \
        ../schemas/$schemafile
}

register 'bq' 'publish_historical' 'publish to bigquery' upload_to_bigquery_historical