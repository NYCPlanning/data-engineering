#!/bin/bash
source bash/config.sh

function dataloading { 
    MODE="${1:-edm}"
    echo "mode: $MODE"
    ./bash/01_dataloading.sh $1
}

function build { 
    ./bash/02_build_devdb.sh 
}

function aggregate {
    ./bash/03_aggregate_unit_change_summary.sh
}

function qaqc { 
    ./bash/04_qaqc.sh 
}

function export { 
    ./bash/05_export.sh 
}

function upload {
    python3 -m dcpy.connectors.edm.publishing upload -p db-developments -a public-read
}

function archive { 
    ./bash/07_archive.sh 
}

function output {
    name=$1
    format=$2
    case $format in 
        csv) csv_export $1;;
        shp) shp_export $1 POINT;;
        *) echo "format: $2 is unknown"
    esac
}

function library_archive {
    local version=$(get_version ${2})
    echo "version of ${1} is ${version}"
    docker run --rm\
        -e AWS_S3_ENDPOINT=$AWS_S3_ENDPOINT\
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID\
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY\
        -e AWS_S3_BUCKET=$recipes_bucket\
        -e CI=$CI\
        -v $(pwd)/templates/$1.yml:/library/$1.yml\
        -v $(pwd)/$1.csv:/library/$1.csv\
        -v $(pwd)/.library:/library/.library\
    nycplanning/library:ubuntu-latest bash -c "
        library archive -f $1.yml -s -l -o csv -v $version &
        library archive -f $1.yml -s -l -o pgdump -v $version &
        wait
    "
}

function library_archive_version {
    local name=$1
    local version=$2
    docker run --rm\
        -e AWS_S3_ENDPOINT=$AWS_S3_ENDPOINT\
        -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID\
        -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY\
        -e AWS_S3_BUCKET=$recipes_bucket\
        -v $(pwd)/templates/$name.yml:/library/$name.yml\
        -v $(pwd)/$name.csv:/library/$name.csv\
        -v $(pwd)/.library:/library/.library\
    nycplanning/library:ubuntu-latest bash -c "
        library archive -f $name.yml -s -l -o csv -v $version &
        library archive -f $name.yml -s -l -o pgdump -v $version &
        wait
    "
}

function import {
    dataset=$1
    version=$2
    import_recipe $dataset $version false
}

function upload_to_bq {
    FILEPATH=dcp_developments/$VERSION/dcp_developments.csv
    curl -O https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$FILEPATH
    gsutil cp dcp_developments.csv gs://edm-temporary/$FILEPATH
    rm dcp_developments.csv
    location=US
    dataset=dcp_developments
    tablename=$dataset.$(tr [A-Z] [a-z] <<< "$VERSION")
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
        schemas/dcp_developments.json
}

function clear {
    rm -rf .library
}

command="$1"
shift

case "${command}" in
    dataloading | build | qaqc | aggregate | export | upload | archive | clear | geocode | import | output | library_archive | library_archive_version) ${command} $@ ;;
    bq) upload_to_bq ;;
    *) echo "${command} not found" ;;
esac
