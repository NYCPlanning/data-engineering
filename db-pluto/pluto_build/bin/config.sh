#!/bin/bash
source ../../bash/utils.sh # assumes being run from pluto_build folder

function import_qaqc {
    name=${1}
    DO_folder=${2}
    target_dir=./.library/qaqc
    qaqc_do_url=https://nyc3.digitaloceanspaces.com/edm-publishing/db-pluto/${DO_folder}/latest/output/qaqc
    if [ -f ${target_dir}/${name}.sql ]; then
      echo "✅ ${name}.sql exists in cache"
    else
      echo "🛠 ${name}.sql doesn't exists in cache, downloading ..."
      mkdir -p ${target_dir} && (
        cd ${target_dir}
        curl -ss -O ${qaqc_do_url}/${name}.sql
      )
    fi
    run_sql_command "DROP TABLE IF EXISTS ${name}"
    run_sql_file ${target_dir}/${name}.sql
}

function shp_export_pluto {
    local filename=${1}
    mkdir ${filename}
    (
        cd ${filename}
        shp_export "$@"
    )
}

function fgdb_export_pluto {
    local filename=${1}
    mkdir ${filename}
    (
        cd ${filename}
        fgdb_export_partial ${filename} MULTIPOLYGON ${filename} ${filename}
        fgdb_export_partial ${filename} NONE NOT_MAPPED_LOTS unmapped -update
        fgdb_export_cleanup ${filename}
    )
}
