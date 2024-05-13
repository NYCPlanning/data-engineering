#!/bin/bash
source bash/config.sh

set -e

function set_env {
    for envfile in $@; do 
        if [ -f $envfile ]; then 
            export $(cat $envfile | sed 's/#.*//g' | xargs) 
        fi 
    done
}

function init {
    set_env
    mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
    psql $BUILD_ENGINE -q -c "
        CREATE TABLE IF NOT EXISTS versions (
            datasource text,
            version text
        );
    "
    record_version "geosupport" "$(get_geosupport_version)"
    record_version "run_date" "$VERSION"
}

function run {
    shift;
    local name=$1
    if [ -f bash/$name.sh ]; then
        shift;
        ./bash/$name.sh $@
    else
        echo "$1 not a valid test, check what's available in the bash folder"
    fi
}

case $1 in 
    init ) init ;;
    run ) run $@ ;;
esac
