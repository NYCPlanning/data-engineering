#!/bin/bash
set -e
source $(pwd)/bin/cli.sh
DATE=$(date "+%Y-%m-%d")

function set_env {
  for envfile in $@
  do
    if [ -f $envfile ]
      then
        export $(cat $envfile | sed 's/#.*//g' | xargs)
      fi
  done
}
set_env .env

function urlparse {
    proto="$(echo $1 | grep :// | sed -e's,^\(.*://\).*,\1,g')"
    url=$(echo $1 | sed -e s,$proto,,g)
    userpass="$(echo $url | grep @ | cut -d@ -f1)"
    BUILD_PWD=`echo $userpass | grep : | cut -d: -f2`
    BUILD_USER=`echo $userpass | grep : | cut -d: -f1`
    hostport=$(echo $url | sed -e s,$userpass@,,g | cut -d/ -f1)
    BUILD_HOST="$(echo $hostport | sed -e 's,:.*,,g')"
    BUILD_PORT="$(echo $hostport | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')"
    BUILD_DB="$(echo $url | grep / | cut -d/ -f2- | cut -d? -f1)"
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
        zip $4.zip *
        ls | grep -v $4.zip | xargs rm
      )
  mv $4/$4.zip $4.zip
  rm -rf $4
}

function CSV_export {
  psql $1  -c "\COPY (
    SELECT * FROM $2
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $3.csv
}

# upload_folder clears the prefix first, so files from an earlier run do not linger
function _upload_output {
  python3 -c "from pathlib import Path; from dcpy.utils import s3; s3.upload_folder('${PUBLISHING_BUCKET:-edm-publishing}', Path('output'), Path('$1'), acl='private', contents_only=True)"
}

function Upload {
  _upload_output ceqr-app-data-staging/$1/$2
}

function Upload_data_operations {
  _upload_output datasets/$1/$2
}
