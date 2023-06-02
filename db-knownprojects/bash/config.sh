#!/bin/bash
if [ -f .env ]
then
  export $(cat .env | sed 's/#.*//g' | xargs)
fi

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

function import_private {
  name=$1
  version=${2:-latest} #default version to latest
  version=$(mc cat spaces/edm-recipes/datasets/$name/$version/config.json | jq -r '.dataset.version')
  echo "$name version: $version"
  mc cp spaces/edm-recipes/datasets/$name/$version/$name.sql $name.sql
  psql $BUILD_ENGINE -f $name.sql
  rm $name.sql
}

function import_public {
  name=$1
  version=${2:-latest}
  version=$(curl -s https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$name/$version/config.json | jq -r '.dataset.version')
  echo "$name version: $version"
  curl -O https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/$name/$version/$name.sql
  psql $BUILD_ENGINE -f $name.sql
  rm $name.sql
}

function CSV_export {
  psql $BUILD_ENGINE  -c "\COPY (
    SELECT * FROM $@
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $@.csv
}

function SHP_export {
  urlparse $BUILD_ENGINE
  table=$1
  geomtype=$2
  name=${3:-$table}
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

function Compress {
  filename=$1
  zip -9 $filename.zip $filename
  rm $filename
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
    psql $EDM_DATA -c "COMMENT ON TABLE $dst IS '$DATE ${commit//\'/}'"
}