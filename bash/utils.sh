#!/bin/bash

DATE=$(date "+%Y-%m-%d")
recipes_bucket=edm-recipes
publishing_bucket=edm-publishing
recipes_url=${AWS_S3_ENDPOINT}/${recipes_bucket}
UTILS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
ROOT_DIR=$(dirname ${UTILS_DIR})
export PYTHONPATH=$ROOT_DIR


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


function set_error_traps {
  # Exit when any command fails
  set -e
  # keep track of the last executed command
  trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
  # echo an error message before exiting
  trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT
}


function run_sql_file {
    psql ${BUILD_ENGINE} --set ON_ERROR_STOP=1 --single-transaction --quiet --file "$@"
}


function run_sql_command {
    psql ${BUILD_ENGINE} --set ON_ERROR_STOP=1 --quiet --command "$@"
}

function sql_table_summary {
  # TODO use utils function for ALL sql commands using BUILD_ENGINE
  # psql -d $BUILD_ENGINE -At -c "SELECT count(*) FROM $1;" | 
  psql -d ${BUILD_ENGINE} -At -c "SELECT count(*) FROM $1;" | 
  while read -a count; do
  echo -e "
  \e[33m$1: $count records\e[0m
  "
  done

  ddl=$(psql -At ${BUILD_ENGINE} -c "SELECT get_DDL('$1') as DDL;")
  echo -e "
  \e[33m$ddl\e[0m
  "
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
        "DROP TABLE IF EXISTS source_data_versions;
        CREATE TABLE source_data_versions (
            schema_name character varying,
            v character varying
        );"
}


function import_recipe {
    local name=${1}
    local version=${2:-latest}
    local set_version_option=${3:-true}
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
    run_sql_file ${target_dir}/${name}.sql
    if [ -n "${BUILD_ENGINE_SCHEMA}" ]; then
        # pgdumb files always create a table in the default "public" schema
        echo "Setting schema of recipe table ${1} to be ${BUILD_ENGINE_SCHEMA}"
        run_sql_command "ALTER TABLE $1 SET SCHEMA ${BUILD_ENGINE_SCHEMA};";
    fi
    run_sql_command \
        "ALTER TABLE ${name} ADD COLUMN data_library_version text; \
        UPDATE ${name} SET data_library_version = '${version}';"
    if [ "${set_version_option}" = true ]; then
        run_sql_command "INSERT INTO source_data_versions VALUES ('$name','$version');";
    fi
}


# pluto does not have null arg
function import_local_csv {
    local filename=${1}
    cat data/${filename}.csv | psql ${BUILD_ENGINE} -c "COPY ${filename} FROM STDIN WITH DELIMITER ',' NULL '' CSV HEADER;"
}


function csv_export {
    local table=${1}
    local output_file=${2:-${table}}
    run_sql_command \
        "\COPY (\
            SELECT * FROM ${table}\
        ) TO STDOUT DELIMITER ',' CSV HEADER;">${output_file}.csv
}


function csv_export_drop_columns {
    local table=${1}
    local columns=${2} #expected in format `csv_export_drop_columns mytable "'geom', 'other_column'"
    local output_file=${3:-${table}}
    if [ -n "${BUILD_ENGINE_SCHEMA}" ]; then
        local schema=${BUILD_ENGINE_SCHEMA}
    else
        local schema="public"
    fi
    
    # TODO schema isn't always public
    local select_columns=$(run_sql_command \
        "\COPY (SELECT '\"' || STRING_AGG(attname, '\",\"' ORDER BY attnum) || '\"'\
        FROM pg_attribute\
        WHERE attrelid = '${schema}.${table}'::regclass\
            AND attname not in (${columns})\
            AND attnum>0
        ) TO STDOUT;")
    run_sql_command \
        "\COPY (\
            SELECT ${select_columns} FROM ${table}\
        ) TO STDOUT DELIMITER ',' CSV HEADER;">${output_file}.csv
}


function shp_export {
    # parse_connection_string ${BUILD_ENGINE}
    local table=${1}
    local geomtype=${2}
    case $3 in
        -f) 
            local filename=$4
            shift 4;;
        *) 
            local filename=$table
            shift 2;;
    esac
    if [ -n "${BUILD_ENGINE_SCHEMA}" ]; then
        local schema=${BUILD_ENGINE_SCHEMA}
    else
        local schema="public"
    fi
    echo "attempting to export table ${schema}.${table} to ${filename}.shp.zip with ${geomtype} geometries ..."
    mkdir -p ${filename} &&(
        cd ${filename}
        ogr2ogr -progress -f "ESRI Shapefile" ${filename}.shp \
            PG:${BUILD_ENGINE} \
            ${schema}.${table} -nlt ${geomtype} "$@"
        rm -f ${filename}.shp.zip
        zip -9 ${filename}.shp.zip *
        ls | grep -v ${filename}.shp.zip | xargs rm
    )
    mv ${filename}/${filename}.shp.zip ${filename}.shp.zip
    rm -rf ${filename}
}


function fgdb_export_partial {
    parse_connection_string ${BUILD_ENGINE}
    local filename=${1}
    local geomtype=${2}
    local nln=${3}
    local table=${4}
    shift 4
    mkdir -p ${filename}.gdb && (
        cd ${filename}.gdb
        ogr2ogr -progress -f "OpenFileGDB" ${filename}.gdb \
            PG:"host=${BUILD_HOST} user=${BUILD_USER} port=${BUILD_PORT} dbname=${BUILD_DB} password=${BUILD_PWD}" \
            -mapFieldType Integer64=Real\
            -lco GEOMETRY_NAME=Shape\
            -nln ${nln}\
            -nlt ${geomtype}\
            ${table} "$@"
        rm -f ${filename}.gdb.zip
    )
}


function fgdb_export_cleanup {
    local filename=${1}
    cd ${filename}.gdb
    zip -r ${filename}.gdb.zip ${filename}.gdb
    rm -rf ${filename}.gdb
    cd ..
    mv ${filename}.gdb/${filename}.gdb.zip ${filename}.gdb.zip
    rm -rf ${filename}.gdb
}


function fgdb_export { 
    parse_connection_string ${BUILD_ENGINE}
    local table=${1}
    local geomtype=${2}
    local filename=${3:-$table}
    fgdb_export_partial ${filename} ${geomtype} ${table} ${table}
    fgdb_export_cleanup ${filename}
}


# only in kpdb currently
function compress {
    filename=${1}
    zip -9 ${filename}.zip ${filename}
    rm ${filename}
}


function upload {
    local dataset_name=${1}
    local acl=${2}
    local branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
    local build=${3:-${branchname}}
    python3 -m dcpy.connectors.edm.publishing upload -p $dataset_name -o output -b $build -a $acl
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
        if [[ ${current_number} -lt ${max_number} ]]; then
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


function docker_login() {
    echo "$DOCKER_PASSWORD" | docker login -u $DOCKER_USER --password-stdin
}


function docker_tag_exists() {
    docker manifest inspect $1:$2 >/dev/null
}


function build_and_publish_docker_image {
    local image_name=${1}
    local version=${2} 
    shift 2
    if docker_tag_exists $image_name $version; then
        echo "$image_name:$version already exist"
    else
        # State version name
        echo "publishing $image_name:$version"

        # Build image
        docker build --tag $image_name:$version "$@"
        # Update Dockerhub
        docker push $image_name:$version
        docker tag $image_name:$version $image_name:latest
        docker push $image_name:latest    
    fi
}
