#!/bin/bash

source ../../bash/utils.sh

set_env .env
set_error_traps


# Setting Environmental Variables
DATE=$(date "+%Y-%m-%d")

function import_qaqc_historic {
    local name="qaqc_historic"
    target_dir=$(pwd)/.library/data/$VERSION
    target_filename="${name}.csv"
    qaqc_do_path=spaces/edm-publishing/db-developments/publish/latest/${target_filename}
    if [ -f ${target_dir}/${target_filename} ]; then
        echo "✅ ${target_filename} exists in cache"
    else
        echo "🛠 ${target_filename} doesn't exists in cache, downloading ..."
        mkdir -p $target_dir && (
        cd $target_dir
        mc cp $qaqc_do_path ${target_filename}
        )
    fi
        run_sql_file sql/qaqc/_create_qaqc_historic.sql
        run_sql_command "\COPY qaqc_historic FROM "${target_dir}/${target_filename}" DELIMITER ',' CSV HEADER;"
}
