#!/bin/bash
# A script used by build scripts to import utility functions, set environment variables,
# and configure connections

function set_error_traps {
  # Exit when any command fails
  set -e
  # Keep track of the last executed command
  trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
  # # Echo an error message before exiting
  # trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT
}

function set_env {
  for envfile in $@; do
    if [ -f $envfile ]; then
      export $(cat $envfile | sed 's/#.*//g' | xargs)
    fi
  done
}

# Setting error traps
set_error_traps

# Setting Environmental Variables
set_env .env version.env

function run_sql {
  psql $BUILD_ENGINE --set ON_ERROR_STOP=1 --file $1
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

function CSV_export {
  psql $1 --set ON_ERROR_STOP=1 -c "\COPY (
    SELECT * FROM $2
  ) TO STDOUT DELIMITER ',' CSV HEADER;" >$3.csv
}

function SHP_export {
  urlparse $1
  mkdir -p $4 &&
    (
      cd $4
      ogr2ogr -progress -f "ESRI Shapefile" $4.shp \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -nlt $3 $2
      rm -f $4.zip
      zip -9 $4.zip *
      ls | grep -v $4.zip | xargs rm
    )
  mv $4/$4.zip $4.zip
  rm -rf $4
}

function import_public {
  local name=$1
  local version=${2:-latest}
  local url=https://nyc3.digitaloceanspaces.com/edm-recipes
  local version=$(curl -ss $url/datasets/$name/$version/config.json | jq -r '.dataset.version')
  echo "$name version: $version"

  local target_dir=$(pwd)/.library/datasets/$name/$version

  # Download sql dump for the datasets from data library
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      local download_url=$url/datasets/$name/$version/$name.sql
      local download_url_zip=$download_url.zip
      local statuscode=$(curl --silent --output $name.sql.zip --write-out "%{http_code}" $download_url_zip)
      if [ $statuscode = 404 ]; then
        curl -ss -O $download_url
      else
        unzip $name.sql.zip
      fi
      rm $name.sql.zip
    )
  fi

  # Loading into Database
  psql $BUILD_ENGINE -f $target_dir/$name.sql
}


# Upload to DigitalOcean
function Upload {
    local BRANCHNAME=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
    local DATE=$(date "+%Y-%m-%d")
    local SPACES="spaces/edm-publishing/db-cbbr/$BRANCHNAME"

    pwd

    mc rm -r --force $SPACES/latest
    mc cp -r ./ $SPACES/latest
    mc rm -r --force $SPACES/$VERSION
    mc cp -r ./ $SPACES/$VERSION
}