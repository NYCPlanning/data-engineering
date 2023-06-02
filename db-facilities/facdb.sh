#!/bin/bash
source facdb/bash/config.sh
max_bg_procs 5

function init {
    docker-compose up -d
    docker-compose exec -T facdb facdb init
}

function facdb_execute {
    docker-compose exec -T facdb facdb $@
}

function facdb_upload {
    mc config host add spaces $AWS_S3_ENDPOINT $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY --api S3v4
    local branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
    local DATE=$(date "+%Y-%m-%d")
    local SPACES="spaces/edm-publishing/db-facilities/$branchname"
    mc rm -r --force $SPACES/latest
    mc cp -r output $SPACES/latest
    mc rm -r --force $SPACES/$DATE
    mc cp -r output $SPACES/$DATE
}

function facdb_archive {
    shift;
    case $1 in
    --all)
        archive public.facdb facdb.facdb &
        archive public.facdb_base facdb.facdb_base &
        archive public.facdb_geom facdb.facdb_geom &
        archive public.facdb_spatial facdb.facdb_spatial
        archive public.facdb_agency facdb.facdb_agency &
        archive public.facdb_address facdb.facdb_address &
        archive public.facdb_classification facdb.facdb_classification
        wait
        echo "Archive Complete"
        ;;
    *) archive $@ ;;
    esac
}

function facdb_export {
    ./facdb/bash/export.sh
}

case $1 in
    init) init ;;
    upload) facdb_upload ;;
    archive) facdb_archive $@ ;;
    export) facdb_export $@ ;;
    *) facdb_execute $@ ;;
esac
