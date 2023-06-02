#!/bin/bash
function set_error_traps {
  # Exit when any command fails
  set -e
  # keep track of the last executed command
  trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
  # echo an error message before exiting
  trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT
}

set_error_traps

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

# Set Date
DATE=$(date "+%Y-%m-%d")
branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
# Set Bucket info
s3_endpoint=https://nyc3.digitaloceanspaces.com
s3_bucket=edm-recipes

function urlparse {
    proto="$(echo $1 | grep :// | sed -e's,^\(.*://\).*,\1,g')"
    url=$(echo $1 | sed -e s,$proto,,g)
    userpass="$(echo $url | grep @ | cut -d@ -f1)"
    BUILD_PWD=`echo $userpass | grep : | cut -d: -f2`
    BUILD_USER=`echo $userpass | grep : | cut -d: -f1`
    hostport=$(echo $url | sed -e s,$userpass@,,g | cut -d/ -f1)
    BUILD_HOST="$(echo $hostport | sed -e 's,:.*,,g')"
    BUILD_PORT="$(echo $hostport | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
    BUILD_DB="$(echo $url | grep / | cut -d/ -f2-)"
}

function FGDB_export {
  urlparse $BUILD_ENGINE
  mkdir -p output/$@.gdb &&
  (cd output/$@.gdb
    docker run \
      --network host\
      -v $(pwd):/data\
      --user $UID\
      --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" $@.gdb \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -mapFieldType Integer64=Real\
        -lco GEOMETRY_NAME=Shape\
        -nln $@\
        -nlt MULTIPOLYGON\
        $@
    docker run \
      --network host\
      -v $(pwd):/data\
      --user $UID\
      --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" $@.gdb \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -mapFieldType Integer64=Real\
        -update -nlt NONE\
        -nln NOT_MAPPED_LOTS\
        unmapped
      rm -f $@.gdb.zip
      zip -r $@.gdb.zip $@.gdb
      rm -rf $@.gdb
    )
}


function SHP_export {
  name=$1
  urlparse $BUILD_ENGINE
  mkdir -p output/$name &&
    (
      cd output/$name
      
      ogr2ogr -progress -f "ESRI Shapefile" $name.shp \
          PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
          $name
      rm -f $name.shp.zip
      zip -9 $name.shp.zip *
      ls | grep -v $name.shp.zip | xargs rm
    )
}


function CSV_export {
  psql $BUILD_ENGINE  -c "\COPY (
    SELECT * FROM $@
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $@.csv
}

function imports_csv {
   cat data/$1.csv | psql $BUILD_ENGINE -c "COPY $1 FROM STDIN DELIMITER ',' CSV HEADER;"
}

function Upload {
  local target_folder=$1
  mc cp -r output spaces/edm-publishing/db-pluto/$target_folder/$DATE
  mc cp -r output spaces/edm-publishing/db-pluto/$target_folder/latest
}

function run {
  psql $BUILD_ENGINE -f $1
}

function get_latest_version {
  name=$1
  latest_version=$(curl -s $s3_endpoint/$s3_bucket/datasets/$1/latest/config.json |  jq -r '.dataset.version')
}

function get_version {
  name=$1
  version=${2:-latest}
  url=https://nyc3.digitaloceanspaces.com/edm-recipes
  echo -e "getting $name $version version ..."
  version=$(curl -sS $url/datasets/$name/$version/config.json | jq -r '.dataset.version')
  echo -e "🔵 $name version: $version"
}

function import_public {
  name=$1
  # version=${2:-latest}
  get_version $1 $2
  target_dir=$(pwd)/.library/datasets/$name/$version

  # Download sql dump for the datasets from data library
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      curl -O $url/datasets/$name/$version/$name.sql
    )
  fi

  # Loading into Database
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -f $target_dir/$name.sql
  psql -1 $BUILD_ENGINE -c "ALTER TABLE $name ADD COLUMN v text; UPDATE $name SET v = '$version';"
}

function import_qaqc {
  name=$1
  DO_folder=$2
  target_dir=$(pwd)/.library/qaqc
  qaqc_do_url=https://nyc3.digitaloceanspaces.com/edm-publishing/db-pluto/$DO_folder/latest/output/qaqc
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      curl -ss -O $qaqc_do_url/$name.sql
    )
  fi
  psql $BUILD_ENGINE -c "DROP TABLE IF EXISTS $name"
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -q -f $target_dir/$name.sql
}
