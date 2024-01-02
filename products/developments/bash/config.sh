#!/bin/bash

source ../../bash/utils.sh

set_env .env version.env
set_error_traps


# Setting Environmental Variables
DATE=$(date "+%Y-%m-%d")

function import_qaqc_historic {
    local name="qaqc_historic"
    target_dir=$(pwd)/.library/data/$VERSION
    target_filename="${name}.csv"
    qaqc_do_path=spaces/edm-publishing/db-developments/main/latest/output/${target_filename}
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

function archive_devdb { ## different from "standard" archive slightly
    echo "archiving $1 -> $2"
    pg_dump -t $1 $BUILD_ENGINE -O -c | psql $EDM_DATA
    psql $EDM_DATA -c "CREATE SCHEMA IF NOT EXISTS $2;";
    psql $EDM_DATA -c "ALTER TABLE $1 SET SCHEMA $2;";
    psql $EDM_DATA -c "DROP VIEW IF EXISTS $2.latest;";
    psql $EDM_DATA -c "DROP TABLE IF EXISTS $2.\"$DATE\";";
    psql $EDM_DATA -c "ALTER TABLE $2.$1 RENAME TO \"$DATE\";";
    psql $EDM_DATA -c "CREATE VIEW $2.latest AS (SELECT '$DATE' as v, * FROM $2.\"$DATE\");"
}

function geocode {
    python3 python/geocode_hpd_hny.py
    python3 python/geocode_hpd_historical.py
    python3 python/geocode_dob.py
}
