#!/bin/bash
source bash/config.sh
set_error_traps

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
    import | output | library_archive) ${command} $@ ;;
    *) echo "${command} not found" ;;
esac
