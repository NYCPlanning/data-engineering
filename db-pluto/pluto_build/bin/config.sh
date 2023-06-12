#!/bin/bash
source ../../bash_utils/config.sh # assumes being run from pluto_build folder

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

function fgdb_export_pluto {
    local filename=${1}
    fgdb_export_partial ${filename} MULTIPOLYGON ${filename} ${filename}
    fgdb_export_partial ${filename} NONE NOT_MAPPED_LOTS unmapped -update
    fgdb_export_cleanup filename
}

function fgdb_export_pluto_docker { # keeping for posterity at the moment
    parse_connection_string ${BUILD_ENGINE}
    table=${1}
    geomtype=${2}
    name=${3:-${table}}
    mkdir -p ${name}.gdb && (
        docker run \
            --network host\
            -v $(pwd):/data\
            --user $UID\
            --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" ${name}.gdb \
                PG:"host=${BUILD_HOST} user=${BUILD_USER} port=${BUILD_PORT} dbname=${BUILD_DB} password=${BUILD_PWD}" \
                -mapFieldType Integer64=Real\
                -lco GEOMETRY_NAME=Shape\
                -nln ${name}\
                -nlt MULTIPOLYGON\
                ${name}
        docker run \
            --network host\
            -v $(pwd):/data\
            --user $UID\
            --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" ${name}.gdb \
                PG:"host=${BUILD_HOST} user=${BUILD_USER} port=${BUILD_PORT} dbname=${BUILD_DB} password=${BUILD_PWD}" \
                -mapFieldType Integer64=Real\
                -update -nlt NONE\
                -nln NOT_MAPPED_LOTS\
                unmapped
        rm -f ${name}.gdb.zip
        zip -r ${name}.gdb.zip ${name}.gdb
        rm -rf ${name}.gdb
    )
    mv ${name}.gdb/${name}.gdb.zip ${name}.gdb.zip
    rm -rf ${name}.gdb
}
