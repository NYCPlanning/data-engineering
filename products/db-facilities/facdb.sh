#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")

source $FILE_DIR/../../bash/utils.sh
set_error_traps
max_bg_procs 5

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

function facdb_upload {
    DATE=$(date "+%Y-%m-%d")
    upload "db-facilities" "latest"
    upload "db-facilities" ${DATE}
}

case $1 in
    init) init ;;
    upload) facdb_upload ;;
    archive) facdb_archive $@ ;;
    export) facdb_export $@ ;;
    *) facdb_execute $@ ;;
esac
