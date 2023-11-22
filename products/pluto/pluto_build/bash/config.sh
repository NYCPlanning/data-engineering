#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh

set_env $ROOT_DIR/.env
set_error_traps

function import_qaqc {
    name=${1}
    DO_folder=${2}
    target_dir=./.library/qaqc
    qaqc_do_url=https://nyc3.digitaloceanspaces.com/edm-publishing/db-pluto/publish/${DO_folder}/qaqc
    if [ -f ${target_dir}/${name}.csv ]; then
        echo "✅ ${name}.csv exists in cache"
    else
        echo "🛠 ${name}.csv doesn't exists in cache, downloading ..."
        mkdir -p ${target_dir} && (
            cd ${target_dir}
            curl -ss -O ${qaqc_do_url}/${name}.csv
        )
    fi
    run_sql_command "DROP TABLE IF EXISTS ${name}"
    python3 -m dcpy.utils.postgres import_csv ${target_dir}/${name}.csv
}

function shp_export_pluto {
    local filename=${1}
    mkdir -p ${filename}
    (
        cd ${filename}
        shp_export "$@"
    )
}

function fgdb_export_pluto {
    local filename=${1}
    local foldername=${filename}.gdb
    mkdir ${foldername}
    (
        cd ${foldername}
        fgdb_export_partial ${filename} MULTIPOLYGON ${filename} ${filename}
        fgdb_export_partial ${filename} NONE NOT_MAPPED_LOTS unmapped -update
        fgdb_export_cleanup ${filename}
    )
}
