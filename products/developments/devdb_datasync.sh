#!/bin/bash
source ../../bash/utils.sh
set_error_traps
install_minio

function library_archive {
    set_error_traps
    local ref_dataset=${2}
    local latest_version=$(python3 -c "from dcpy.lifecycle.connector_registry import connectors; print(connectors.versioned['edm.recipes.datasets'].get_latest_version('$ref_dataset'))")
    local version=${3:-$latest_version}
    echo "version of ${1} is ${version}"

    library archive -f templates/$1.yml -s -l -v $version
}

function import {
    dataset=$1
    version=${2:-latest}
    import_recipe $dataset $version false
}

function import_ingest {
    local dataset=$1
    local version=$(python3 -c "from dcpy.lifecycle.connector_registry import connectors; print(connectors.versioned['edm.recipes.datasets'].get_latest_version('$dataset'))")
    python3 -m dcpy lifecycle data_loader import -n $dataset -v "$version" -s public -d postgres
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

command="$1"
shift

case "${command}" in
    import | import_ingest | output | library_archive) ${command} $@ ;;
    *) echo "${command} not found" ;;
esac
