#!/bin/bash
VERSION=$DATE

FILE_DIR=$(dirname "$(readlink -f "$0")")
ROOT_DIR="${FILE_DIR}/../../.."

source $ROOT_DIR/bash/utils.sh
urlparse $BUILD_ENGINE

function parse_flags {
  while [[ "$#" -gt 0 ]]; do
    case $1 in
        -i|--import) import_data=1 ;;
        -p|--python) python_script=1 ;;
        -u|--upload) upload_data=1 ;;
        -e|--export) export_data=1 ;;
        *) 
          echo 
          echo "Please enter valid flags: ./gru.sh run {{ name }} --import|-i --python|-p --export|-e --upload|-u"; 
          echo
          exit 1 ;;
    esac
    shift
  done
}

function get_geosupport_version {
  regex="version-(.*)_([0-9]+)\.([0-9]+)\/"

  cd $GEOFILES
  filename=$(pwd)

  if [[ $filename =~ $regex ]]
  then
    echo "${BASH_REMATCH[1]}"
  else
    echo "$GEOFILES doesn't match" 
  fi
}

function Upload {
  local name=$1
  local version=$2
  local geosupport_version=$(get_geosupport_version)
  mc rm -r --force spaces/edm-publishing/db-gru-qaqc/$geosupport_version/$name/$version/
  for file in $(ls output/$name)
  do
    filename=$(basename $file)
    mc cp output/$name/$filename spaces/edm-publishing/db-gru-qaqc/$geosupport_version/$name/$version/$filename
  done
}

function record_version {
  local datasource="$1"
  local version="$2"
  psql $BUILD_ENGINE -q -c "
  DELETE FROM versions WHERE datasource = '$datasource';
  INSERT INTO versions VALUES ('$datasource', '$version');
  "
}
