#!/bin/bash
set -e

function set_env {
  for envfile in $@; do
    if [ -f $envfile ]; then
      export $(cat $envfile | sed 's/#.*//g' | xargs)
    fi
  done
}

function urlparse {
  proto="$(echo $1 | grep :// | sed -e's,^\(.*://\).*,\1,g')"
  url=$(echo $1 | sed -e s,$proto,,g)
  userpass="$(echo $url | grep @ | cut -d@ -f1)"
  BUILD_PWD=$(echo $userpass | grep : | cut -d: -f2)
  BUILD_USER=$(echo $userpass | grep : | cut -d: -f1)
  hostport=$(echo $url | sed -e s,$userpass@,,g | cut -d/ -f1)
  BUILD_HOST="$(echo $hostport | sed -e 's,:.*,,g')"
  BUILD_PORT="$(echo $hostport | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
  BUILD_DB="$(echo $url | grep / | cut -d/ -f2-)"
}
urlparse $BUILD_ENGINE

function SHP_export {
  mkdir -p $@ &&
    (
      cd $@
      ogr2ogr -progress -f "ESRI Shapefile" $@.shp \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        $@ -nlt MULTIPOLYGON
      rm -f $@.zip
      zip $@.zip *
      ls | grep -v $@.zip | xargs rm
    )
}

function CSV_export {
  local table_name=$1
  local output_name=${2:-$1}
  psql $BUILD_ENGINE -c "\COPY (
    SELECT * FROM $table_name
  ) TO STDOUT DELIMITER ',' CSV HEADER;" >$output_name.csv
}

function Upload {
  mc rm -r --force spaces/edm-publishing/db-zoningtaxlots/$@
  mc cp -r output spaces/edm-publishing/db-zoningtaxlots/$@
}

function get_acl {
  local name=$1
  local version=${2:-latest} #default version to latest
  local config_curl=$URL/datasets/$name/$version/config.json
  local statuscode=$(curl --write-out '%{http_code}' --silent --output /dev/null $config_curl)
  if [[ "$statuscode" -ne 200 ]]; then
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
  if [ "$acl" != "public-read" ]; then
    local version=$(mc cat $config_mc | jq -r '.dataset.version')
  else
    local version=$(curl -sS $config_curl | jq -r '.dataset.version')
  fi
  echo "$version"
}

function record_version {
  local datasource="$1"
  local version="$2"
  psql $BUILD_ENGINE -q -c "
  DELETE FROM versions WHERE datasource = '$datasource';
  INSERT INTO versions VALUES ('$datasource', '$version');
  "
}

function get_existence {
  local name=$1
  local version=${2:-latest} #default version to latest
  existence=$(psql $BUILD_ENGINE -t -c "
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE  table_schema = 'public'
      AND    table_name   = '$name'
    ) and EXISTS (
      SELECT FROM versions
      WHERE  datasource = '$name'
      AND    version   = '$version'
    );
  ")
  echo $existence
}

function import {
  local name=$1
  local version=${2:-latest} #default version to latest
  local acl=$(get_acl $name $version)
  local version=$(get_version $name $version $acl)
  local existence=$(get_existence $name $version)
  local target_dir=$(pwd)/.library/datasets/$name/$version
  # Download sql dump for the datasets from data library
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      if [ "$acl" != "public-read" ]; then
        mc cp spaces/edm-recipes/datasets/$name/$version/$name.sql $name.sql
      else
        curl -O $URL/datasets/$name/$version/$name.sql $name.sql
      fi
    )
  fi
  # Loading into Database
  if [ "$existence" == "t" ]; then
    echo "NAME: $name VERSION: $version is already loaded in postgres!"
  else
    psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f $target_dir/$name.sql
  fi
  record_version "$name" "$version"
}

# Function to run a sql file
function run_sql_file {
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 --file $1
}

# Set Environmental variables
set_env .env version.env

# Parse URL
urlparse $BUILD_ENGINE

# Set Date
DATE=$(date "+%Y/%m/01")
VERSION=$DATE
VERSION_PREV=$(date --date="$(date "+%Y/%m/01") - 1 month" "+%Y/%m/01")

function archive {
  local src=$1
  local dst=${2-$src}
  local src_schema="$(cut -d'.' -f1 <<<"$src")"
  local src_table="$(cut -d'.' -f2 <<<"$src")"
  local dst_schema="$(cut -d'.' -f1 <<<"$dst")"
  local dst_table="$(cut -d'.' -f2 <<<"$dst")"
  local commit="$(git log -1 --oneline)"
  local DATE=$(date "+%Y-%m-%d")
  echo "Dumping $src_schema.$src_table to $dst_schema.$dst_table"
  psql $EDM_DATA -c "CREATE SCHEMA IF NOT EXISTS $dst_schema;"
  pg_dump $BUILD_ENGINE -t $src -O -c | sed "s/$src/$dst/g" | psql $EDM_DATA
  psql $EDM_DATA -c "COMMENT ON TABLE $dst IS '$DATE $commit'"
}
