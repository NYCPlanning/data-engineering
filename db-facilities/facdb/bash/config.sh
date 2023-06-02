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
      if [ $statuscode = 404 ] ; then
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

function CSV_export {
  local name=$1
  local output_name=${2:-$name}
  psql $BUILD_ENGINE  -c "\COPY (
    SELECT * FROM $name
  ) TO STDOUT DELIMITER ',' CSV HEADER;" > $output_name.csv
}

function SHP_export {
  local table=$1
  local geomtype=$2
  local name=${3:-$table}
  mkdir -p $name &&(
    cd $name
    ogr2ogr -progress -f "ESRI Shapefile" $name.shp \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -s_srs EPSG:4326 -t_srs EPSG:2263\
        $table -nlt $geomtype
      rm -f $name.shp.zip
      zip -9 $name.shp.zip *
      ls | grep -v $name.shp.zip | xargs rm
  )
  mv $name/$name.shp.zip $name.shp.zip
  rm -rf $name
}

function FGDB_export {
  local table=$1
  local geomtype=$2
  local name=${3:-$table}
  mkdir -p $name &&(
    cd $name
    docker run \
      -v $(pwd):/data\
      --user $UID\
      --network host\
      --rm webmapp/gdal-docker:latest ogr2ogr -progress -f "FileGDB" $name.gdb \
        PG:"host=$BUILD_HOST user=$BUILD_USER port=$BUILD_PORT dbname=$BUILD_DB password=$BUILD_PWD" \
        -mapFieldType Integer64=Real\
        -s_srs EPSG:4326 -t_srs EPSG:2263\
        -lco GEOMETRY_NAME=Shape\
        -nlt $geomtype $table
    rm -f $name.gdb.zip
    zip -r -9 $name.gdb.zip $name.gdb
  )
  mv $name/$name.gdb.zip $name.gdb.zip
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
