#!/bin/bash
#
# A collection of utility functions used in build scripts.

# Exit when any command fails
set -e
# Keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# Echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT

# Function to set Environmental Variables
function set_env {
  for envfile in $@; do
    if [ -f $envfile ]; then
      export $(cat $envfile | sed 's/#.*//g' | xargs)
    fi
  done
}

# Function to run a sql command from a string
function run_sql_command {
  psql $BUILD_ENGINE --quiet --command "$1"
}

# Function to run a sql file
function run_sql_file {
  psql $BUILD_ENGINE --set ON_ERROR_STOP=1 --file $1
}

# Function to parse the database url and set relevant environment variables
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

# Function to get authorization levels for each dataset (public read vs private)
function get_acl {
  local name=$1
  local version=$2
  local config_curl=$URL/datasets/$name/$version/config.json
  local statuscode=$(curl --write-out '%{http_code}' --silent --output /dev/null $config_curl)
  if [[ "$statuscode" -ne 200 ]]; then
    echo "private"
  else
    echo "public-read"
  fi
}

# Function to get version of dataset from Digital Ocean EDM Recipes
function get_version {
  local name=$1
  local version=${2:-latest} #default version to latest
  local acl=${3:-public-read}
  local config_curl=$URL/datasets/$name/$version/config.json
  local config_mc=spaces/edm-recipes/datasets/$name/$version/config.json
  if [ "$acl" != "public-read" ]; then
    echo "get_version: using non-public read approach ..."
    local version=$(mc cat $config_mc | jq -r '.dataset.version')
  else
    echo "get_version: using public read approach ..."
    local version=$(curl -sS $config_curl | jq -r '.dataset.version')
  fi
  echo "$version"
}

# Function to update a datset's version record
function record_version {
  local datasource="$1"
  local version="$2"
  psql $BUILD_ENGINE -q -c "
  DELETE FROM source_versions WHERE datasource = '$datasource';
  INSERT INTO source_versions VALUES ('$datasource', '$version');
  "
}

# Fucntion to check if the data has already been loaded into the database
function get_existence {
  local name=$1
  local version=$2
  existence=$(psql $BUILD_ENGINE -t -c "
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE  table_schema = 'public'
      AND    table_name   = '$name'
    ) and EXISTS (
      SELECT FROM source_versions
      WHERE  datasource = '$name'
      AND    version   = '$version'
    );
  ")
  echo $existence
}

function import_public {
  name=$1
  version=${2:-latest}
  local url=$DO_S3_ENDPOINT/edm-recipes
  local target_data=datasets/$name/$version
  local target_dir=$(pwd)/.library/$target_data
  local target_file=$target_dir/$name.sql
  local download_url=$url/$target_data/$name.sql
  local config_url=$URL/datasets/$name/$version/config.json

  # Download sql dump for the datasets from data library
  if [ -f $target_file ]; then
    echo "✅ $target_data exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      echo "try to curl $download_url ..."
      local statuscode=$(curl --write-out '%{http_code}' --silent --output /dev/null $config_url)
      echo "statuscode: $statuscode"
      if [ $statuscode == 000 ]; then
        curl -ss -O $download_url
      else
        unzip $name.sql.zip
      fi
    )
  fi

  # Load into database from download
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -q -f $target_file
  record_version $name $version
}
