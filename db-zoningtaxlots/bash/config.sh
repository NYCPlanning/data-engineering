#!/bin/bash

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
