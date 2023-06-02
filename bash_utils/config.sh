#!/bin/bash

DATE=$(date "+%Y-%m-%d")
recipes_bucket=edm-recipes
publishing_bucket=edm-publishing
recipes_url=${AWS_S3_ENDPOINT}/${recipes_bucket}


# Pretty print messages
function display {
    echo -e "
        \e[92m\e[1m$@\e[21m\e[0m
        "
}


function set_env {
    for envfile in $@; do
        if [ -f ${envfile} ]; then
            export $(cat $envfile | sed 's/#.*//g' | xargs)
        fi
    done
}


function run_sql_file {
    local filename=${1}
    psql ${BUILD_ENGINE} --set ON_ERROR_STOP=1 --file ${filename}
}


function run_sql_command {
    local command=${1}
    psql "${BUILD_ENGINE}" --set ON_ERROR_STOP=1  --quiet --command "${command}"
}


# currently only in dev db. Seems nice though for local development
function psql_count_and_ddl {
    table=${1}
    psql -d ${BUILD_ENGINE} -At -c "SELECT count(*) FROM ${table};" | 
    while read -a count; do
        echo -e "
            \e[33m${1}: ${count} records\e[0m
            "
    done

    ddl=$(psql -At ${BUILD_ENGINE} -c "SELECT get_DDL('${table}') as DDL;")
    echo -e "
        \e[33m${ddl}\e[0m
        "
}


function parse_connection_string {
    local connection_string=${1}
    local proto="$(echo ${connection_string} | grep :// | sed -e's,^\(.*://\).*,\1,g')"
    local url=$(echo ${connection_string} | sed -e s,${proto},,g)
    local userpass="$(echo ${url} | grep @ | cut -d@ -f1)"
    BUILD_PWD=`echo ${userpass} | grep : | cut -d: -f2`
    BUILD_USER=`echo ${userpass} | grep : | cut -d: -f1`
    local hostport=$(echo ${url} | sed -e s,${userpass}@,,g | cut -d/ -f1)
    BUILD_HOST="$(echo ${hostport} | sed -e 's,:.*,,g')"
    BUILD_PORT="$(echo ${hostport} | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
    BUILD_DB="$(echo ${url} | grep / | cut -d/ -f2-)"
}


function get_acl {
    local name=${1}
    local version=${2:-latest} #default version to latest
    local config_curl=${recipes_url}/datasets/${name}/${version}/config.json
    local statuscode=$(curl --write-out '%{http_code}' --silent --output /dev/null ${config_curl})
    if [[ "${statuscode}" -ne 200 ]] ; then
        echo "private"
    else
        echo "public-read"
    fi
}


function get_version {
    local name=${1}
    local version=${2:-latest}
    local acl=${3:-public-read}
    local config_curl=${recipes_url}/datasets/${name}/${version}/config.json
    local config_mc=spaces/edm-recipes/datasets/${name}/${version}/config.json
    if [ "${acl}" != "public-read" ] ; then
        local version=$(mc cat ${config_mc} | jq -r '.dataset.version')
    else
        local version=$(curl -sS ${config_curl} | jq -r '.dataset.version')
    fi
    echo "${version}"
}


function create_source_data_table {
    run_sql_command \
        "CREATE TABLE source_data_versions (
            schema_name character varying,
            v character varying
        );"
}


function import_recipe {
    local name=${1}
    local version=${2:-latest}
    local acl=$(get_acl ${name} ${version})
    local version=$(get_version ${name} ${version} ${acl})
    target_dir=./.library/datasets/${name}/${version}

    # Download sql dump for the datasets from data library
    if [ -f ${target_dir}/${name}.sql ]; then
        echo "✅ ${name}.sql exists in cache"
    else
        echo "🛠 ${name}.sql doesn't exists in cache, downloading ..."
        mkdir -p ${target_dir} && (
            cd ${target_dir}
            if [ "${acl}" != "public-read" ] ; then
                mc cp spaces/${recipes_bucket}/datasets/${name}/${version}/${name}.sql $name.sql
            else
                curl -ss -O ${recipes_url}/datasets/${name}/${version}/${name}.sql
            fi
        )
    fi

    # Loading into Database
    run_sql_file $target_dir/${name}.sql
    run_sql_command \
        "ALTER TABLE ${name} ADD COLUMN v text; \
        UPDATE ${name} SET v = '${version}'; \
        INSERT INTO source_data_versions VALUES ('$name','$version');";
}


# pluto does not have null arg
function import_local_csv {
    local filename=${1}
    cat data/${filename}.csv | psql ${BUILD_ENGINE} -c "COPY ${filename} FROM STDIN WITH DELIMITER ',' NULL '' CSV HEADER;"
}


function csv_export {
    local connection_string=${1}
    local table=${2}
    local output_file=${2:-${table}}
    run_sql_command \
        "\COPY ( \
            SELECT * FROM ${table} \
        ) TO STDOUT DELIMITER ',' CSV HEADER;" \ 
        >${output_file}.csv
}


function shp_export {
    urlparse ${BUILD_ENGINE}
    local table=${1}
    local geomtype=${2}
    local filename=${3:-$table}
    mkdir -p ${filename} &&(
        cd ${filename}
        ogr2ogr -progress -f "ESRI Shapefile" ${filename}.shp \
            PG:"host=${BUILD_HOST} user=${BUILD_USER} port=${BUILD_PORT} dbname=${BUILD_DB} password=${BUILD_PWD}" \
            ${table} -nlt ${geomtype}
        rm -f ${filename}.shp.zip
        zip -9 ${filename}.shp.zip *
        ls | grep -v ${filename}.shp.zip | xargs rm
    )
    mv ${filename}/${filename}.shp.zip ${filename}.shp.zip
    rm -rf ${filename}
}


#colp, facdb
function fgdb_export {
    urlparse ${BUILD_ENGINE}
    table=${1}
    geomtype=${2}
    name=${3:-${table}}
    mkdir -p ${name}.gdb && (
        cd ${name}.gdb
        ogr2ogr -progress -f "FileGDB" ${name}.gdb \
            PG:"host=${BUILD_HOST} user=${BUILD_USER} port=${BUILD_PORT} dbname=${BUILD_DB} password=${BUILD_PWD}" \
            -mapFieldType Integer64=Real\
            -lco GEOMETRY_NAME=Shape\
            -nln ${name}\
            -nlt ${geomtype} ${name}
        rm -f ${name}.gdb.zip
        zip -r ${name}.gdb.zip ${name}.gdb
        rm -rf ${name}.gdb
    )
    mv ${name}.gdb/${name}.gdb.zip ${name}.gdb.zip
    rm -rf ${name}.gdb
}


# only in kpdb currently
function compress {
    filename=${1}
    zip -9 ${filename}.zip ${filename}
    rm ${filename}
}


function upload {
    local dataset_name=${1}
    local version=${2}
    local branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
    local SPACES="spaces/${publishing_bucket}/${dataset_name}/${branchname}"
    mc rm -r --force ${SPACES}/${version}
    mc cp -r output ${SPACES}/${version}
}


# cpdb immediately calls with 5 as arg. Similar for devdb, facdb
function max_bg_procs {
    if [[ $# -eq 0 ]] ; then
        echo "Usage: max_bg_procs NUM_PROCS.  Will wait until the number of background (&)"
        echo "           bash processes (as determined by 'jobs -pr') falls below NUM_PROCS"
        return
    fi
    local max_number=$((0 + ${1:-0}))
    while true; do
        local current_number=$(jobs -pr | wc -l)
        if [[ $c{urrent_number} -lt ${max_number} ]]; then
            break
        fi
        sleep 1
    done
}


# cpdb/facdb/kpdb/ztl edm_data archive
function archive {
    local src=${1}
    local dst=${2-$src}
    local src_schema="$(cut -d'.' -f1 <<< "${src}")"
    local src_table="$(cut -d'.' -f2 <<< "${src}")"
    local dst_schema="$(cut -d'.' -f1 <<< "${dst}")"
    local dst_table="$(cut -d'.' -f2 <<< "${dst}")"
    local commit="$(git log -1 --oneline)"
    local DATE=$(date "+%Y-%m-%d")
    echo "Dumping ${src_schema}.${src_table} to ${dst_schema}.${dst_table}"
    psql ${EDM_DATA} -c "CREATE SCHEMA IF NOT EXISTS ${dst_schema};"
    pg_dump ${BUILD_ENGINE} -t ${src} -O -c | sed "s/${src}/${dst}/g" | psql ${EDM_DATA}
    psql ${EDM_DATA} -c "COMMENT ON TABLE ${dst} IS '${DATE} ${commit}'"
}


# devdb dedm_data archive
function archive {
    echo "archiving $1 -> $2"
    pg_dump -t $1 ${BUILD_ENGINE} -O -c | psql ${EDM_DATA}
    psql ${EDM_DATA} -c "CREATE SCHEMA IF NOT EXISTS $2;";
    psql ${EDM_DATA} -c "ALTER TABLE $1 SET SCHEMA $2;";
    psql ${EDM_DATA} -c "DROP VIEW IF EXISTS $2.latest;";
    psql ${EDM_DATA} -c "DROP TABLE IF EXISTS $2.\"${DATE}\";";
    psql ${EDM_DATA} -c "ALTER TABLE $2.$1 RENAME TO \"${DATE}\";";
    psql ${EDM_DATA} -c "CREATE VIEW $2.latest AS (SELECT '${DATE}' as v, * FROM $2.\"$DATE\");"
}