#!/bin/bash

# Setting environmental variables
function set_env {
  for envfile in $@
  do
    if [ -f $envfile ]
      then
        export $(cat $envfile | sed 's/#.*//g' | xargs)
      fi
  done
}

# Setting Environmental Variables
set_env .env
DATE=$(date "+%Y-%m-%d")

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
urlparse $BUILD_ENGINE

function get_version {
  name=$1
  version=${2:-latest}
  url=https://nyc3.digitaloceanspaces.com/edm-recipes
  version=$(curl -s $url/datasets/$name/$version/config.json | jq -r '.dataset.version')
  echo -e "🔵 $name version: \e[92m\e[1m$version\e[21m\e[0m"
}

function import_public {
  name=$1
  version=${2:-latest}
  get_version $1 $2
  target_dir=$(pwd)/.library/datasets/$name/$version

  # Download sql dump for the datasets from data library
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      curl -ss -O $url/datasets/$name/$version/$name.sql
    )
  fi

  # Loading into Database
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -q -f $target_dir/$name.sql
  psql $BUILD_ENGINE -c "ALTER TABLE $name ADD COLUMN v text; UPDATE $name SET v = '$version';"
}

function CSV_export {
  psql $BUILD_ENGINE  -c "\COPY (
    SELECT * FROM $@
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $@.csv
}

function SHP_export {
  table=$1
  geomtype=$2
  name=${3:-$table}
  mkdir -p $name &&(
    cd $name
    docker run \
      --network host\
      -v $(pwd):/data\
      --user $UID\
      --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "ESRI Shapefile" $name.shp \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        $table -nlt $geomtype
      rm -f $name.shp.zip
      zip -9 $name.shp.zip *
      ls | grep -v $name.shp.zip | xargs rm
  )
  mv $name/$name.shp.zip $name.shp.zip
  rm -rf $name
}

function FGDB_export {
  table=$1
  geomtype=$2
  name=${3:-$table}
  mkdir -p $name.gdb &&
  (cd $name.gdb
    docker run \
      --network host\
      -v $(pwd):/data\
      --user $UID\
      --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" $name.gdb \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -mapFieldType Integer64=Real\
        -lco GEOMETRY_NAME=Shape\
        -nln $name\
        -nlt $geomtype $name
      rm -f $name.gdb.zip
      zip -r $name.gdb.zip $name.gdb
      rm -rf $name.gdb
  )
  mv $name.gdb/$name.gdb.zip $name.gdb.zip
  rm -rf $name.gdb
}

function Upload {
  local branchname=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)
  local SPACES="spaces/edm-publishing/db-colp/$branchname"
  mc rm -r --force $SPACES/$@
  mc cp -r output $SPACES/$@
}