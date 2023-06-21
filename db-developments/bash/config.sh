#!/bin/bash
source ../bash_utils/config.sh # expected to be run from db-developments folder

# Setting Environmental Variables
set_env .env version.env
DATE=$(date "+%Y-%m-%d")
set_error_traps

function sql_table_summary {
  psql -d $BUILD_ENGINE -At -c "SELECT count(*) FROM $1;" | 
  while read -a count; do
  echo -e "
  \e[33m$1: $count records\e[0m
  "
  done

  ddl=$(psql -At $BUILD_ENGINE -c "SELECT get_DDL('$1') as DDL;")
  echo -e "
  \e[33m$ddl\e[0m
  "
}

function import_qaqc_historic {
  target_dir=$(pwd)/.library/qaqc/$VERSION
  qaqc_do_path=spaces/edm-publishing/db-developments/main/latest/output/qaqc_historic.sql
  if [ -f $target_dir/$name.sql ]; then
    echo "✅ $name.sql exists in cache"
  else
    echo "🛠 $name.sql doesn't exists in cache, downloading ..."
    mkdir -p $target_dir && (
      cd $target_dir
      mc cp $qaqc_do_path qaqc_historic.sql
    )
  fi
  psql $BUILD_ENGINE -c 'DROP TABLE IF EXISTS qaqc_historic'
  psql $BUILD_ENGINE -v ON_ERROR_STOP=1 -q -f $target_dir/qaqc_historic.sql
}

function archive_devdb { ## different from "standard" archive slightly
    echo "archiving $1 -> $2"
    pg_dump -t $1 $BUILD_ENGINE -O -c | psql $EDM_DATA
    psql $EDM_DATA -c "CREATE SCHEMA IF NOT EXISTS $2;";
    psql $EDM_DATA -c "ALTER TABLE $1 SET SCHEMA $2;";
    psql $EDM_DATA -c "DROP VIEW IF EXISTS $2.latest;";
    psql $EDM_DATA -c "DROP TABLE IF EXISTS $2.\"$DATE\";";
    psql $EDM_DATA -c "ALTER TABLE $2.$1 RENAME TO \"$DATE\";";
    psql $EDM_DATA -c "CREATE VIEW $2.latest AS (SELECT '$DATE' as v, * FROM $2.\"$DATE\");"
}

function geocode {
  docker run --network=host --rm\
      -v $(pwd):/src\
      -w /src\
      -e BUILD_ENGINE=$BUILD_ENGINE\
      nycplanning/docker-geosupport:$GEOSUPPORT_DOCKER_IMAGE_VERSION bash -c "
        python3 python/geocode.py
        python3 python/geocode_hny.py
      "
}