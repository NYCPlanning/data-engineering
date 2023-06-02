#!/bin/bash
function set_env {
  for envfile in $@
  do
    if [ -f $envfile ]
      then
        export $(cat $envfile | sed 's/#.*//g' | xargs)
      fi
  done
}
# Set Environmental variables
set_env .env version.env

URL=https://nyc3.digitaloceanspaces.com/edm-recipes

function urlparse {
    local proto="$(echo $1 | grep :// | sed -e's,^\(.*://\).*,\1,g')"
    local url=$(echo $1 | sed -e s,$proto,,g)
    local userpass="$(echo $url | grep @ | cut -d@ -f1)"
    BUILD_PWD=`echo $userpass | grep : | cut -d: -f2`
    BUILD_USER=`echo $userpass | grep : | cut -d: -f1`
    local hostport=$(echo $url | sed -e s,$userpass@,,g | cut -d/ -f1)
    BUILD_HOST="$(echo $hostport | sed -e 's,:.*,,g')"
    BUILD_PORT="$(echo $hostport | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
    BUILD_DB="$(echo $url | grep / | cut -d/ -f2-)"
}
urlparse $BUILD_ENGINE

function max_bg_procs {
    if [[ $# -eq 0 ]] ; then
            echo "Usage: max_bg_procs NUM_PROCS.  Will wait until the number of background (&)"
            echo "           bash processes (as determined by 'jobs -pr') falls below NUM_PROCS"
            return
    fi
    local max_number=$((0 + ${1:-0}))
    while true; do
            local current_number=$(jobs -pr | wc -l)
            if [[ $current_number -lt $max_number ]]; then
                    break
            fi
            sleep 1
    done
}
max_bg_procs 5

function get_acl {
  local name=$1
  local version=${2:-latest} #default version to latest
  local config_curl=$URL/datasets/$name/$version/config.json
  local statuscode=$(curl --write-out '%{http_code}' --silent --output /dev/null $config_curl)
  if [[ "$statuscode" -ne 200 ]] ; then
    echo "private"
  else
    echo "public-read"
  fi
}


function get_version {
  local name=$1
  local version=${2:-latest} #default version to latest
  local acl=${3:-public-read}
  local config_curl=$URL/datasets/$name/$version/config.json
  local config_mc=spaces/edm-recipes/datasets/$name/$version/config.json
  if [ "$acl" != "public-read" ] ; then
    local version=$(mc cat $config_mc | jq -r '.dataset.version')
  else
    local version=$(curl -sS $config_curl | jq -r '.dataset.version')
  fi
  echo "$version"
}

function create_source_data_table {
  psql $BUILD_ENGINE --set ON_ERROR_STOP=1 -c \
  "CREATE TABLE source_data_versions (
    schema_name character varying,
    v character varying
  );"
}

function import {
  local name=$1
  local version=${2:-latest} #default version to latest
  local acl=$(get_acl $name $version)
  local version=$(get_version $name $version $acl)
  local target_dir=$(pwd)/.library/datasets/$name/$version
  # Download sql dump for the datasets from data library
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      if [ "$acl" != "public-read" ] ; then
        mc cp spaces/edm-recipes/datasets/$name/$version/$name.sql $name.sql
      
      else
        curl -sS -O $URL/datasets/$name/$version/$name.sql $name.sql
      fi
    )
  fi
  # Loading into Database
  psql $BUILD_ENGINE -f $target_dir/$name.sql
  psql $BUILD_ENGINE -c \
    "ALTER TABLE $name ADD COLUMN v text; \
    UPDATE $name SET v = '$version'; \
    INSERT INTO source_data_versions VALUES ('$name','$version');";
}


# Function to run a sql command from a string
function run_sql_command {
  psql "${BUILD_ENGINE}" --set ON_ERROR_STOP=1  --quiet --command "$1"
}


# Function to run a sql file
function run_sql_file {
  psql "${BUILD_ENGINE}" --set ON_ERROR_STOP=1 --file "$1"
}


function CSV_export {
  local name=$1
  local output_name=${2:-$name}
  psql $BUILD_ENGINE  -c "\COPY (
    SELECT * FROM $name
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $output_name.csv
}

function SHP_export {
  urlparse $BUILD_ENGINE
  local table=$1
  local geomtype=$2
  local name=${3:-$table}
  mkdir -p $name &&(
    cd $name
    ogr2ogr -progress -f "ESRI Shapefile" $name.shp \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        $table -nlt $geomtype
      rm -f $name.shp.zip
      zip -9 $name.shp.zip *
      ls | grep -v $name.shp.zip | xargs rm
  )
  mv $name/$name.shp.zip $name.shp.zip
  rm -rf $name
}

function archive {
    local src=$1
    local dst=${2-$src}
    local src_schema="$(cut -d'.' -f1 <<< "$src")"
    local src_table="$(cut -d'.' -f2 <<< "$src")"
    local dst_schema="$(cut -d'.' -f1 <<< "$dst")"
    local dst_table="$(cut -d'.' -f2 <<< "$dst")"
    local commit="$(git log -1 --oneline)"
    local DATE=$(date "+%Y-%m-%d")
    echo "Dumping $src_schema.$src_table to $dst_schema.$dst_table"
    psql $EDM_DATA -c "CREATE SCHEMA IF NOT EXISTS $dst_schema;"
    pg_dump $BUILD_ENGINE -t $src -O -c | sed "s/$src/$dst/g" | psql $EDM_DATA
    psql $EDM_DATA -c "COMMENT ON TABLE $dst IS '$DATE $commit'"
}