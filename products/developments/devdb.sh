#!/bin/bash
source bash/config.sh
set_error_traps

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
    set_error_traps
    local latest_version=$(get_version ${2})
    local version=${3:-$latest_version}
    echo "version of ${1} is ${version}"
    
    library archive -f templates/$1.yml -s -l -v $version
}

function import {
    dataset=$1
    version=${2:-latest}
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
    dataloading | build | qaqc | aggregate | export | upload | archive | clear | import | output | library_archive) ${command} $@ ;;
    bq) upload_to_bq ;;
    *) echo "${command} not found" ;;
esac
