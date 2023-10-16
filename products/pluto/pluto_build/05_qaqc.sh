#!/bin/bash
source ./bash/config.sh
set_error_traps

# Download Existing QAQC from DO
# HACK due to change in build DB, dm_temp_for_edm_db must be used rather than latest
# until PLUTO 23v3 is published
import_qaqc qaqc_expected dm_temp_for_edm_db
import_qaqc qaqc_aggregate dm_temp_for_edm_db
import_qaqc qaqc_mismatch dm_temp_for_edm_db
import_qaqc qaqc_null dm_temp_for_edm_db
import_qaqc qaqc_outlier dm_temp_for_edm_db

wait

# QAQC EXPECTED VALUE ANALYSIS
run_sql_file sql/qaqc_expected.sql -v VERSION=${VERSION}

function set_condition {
  mapped=${1} 
  condo=${2}
  if [ "${mapped}" = true ] && [ "${condo}" = true ] ; then
    export condition="WHERE right(a.bbl::bigint::text, 4) LIKE '75%%' AND a.geom IS NOT NULL"
  elif [ "${mapped}" = true ] && [ "${condo}" = false ] ; then
    export condition="WHERE a.geom IS NOT NULL"
  elif [ "${mapped}" = false ] && [ "${condo}" = true ] ; then
    export condition="WHERE right(a.bbl::bigint::text, 4) LIKE '75%%'"
  elif [ "${mapped}" = false ] && [ "${condo}" = false ] ; then
    export condition=""
  fi
}

function QAQC {
  echo "Running ${1}"
  file=${1}
  mapped=${2}
  condo=${3}
  set_condition ${mapped} ${condo}
  run_sql_file ${file} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} -v CONDO=${condo} \
  -v MAPPED=${mapped}  -v CONDITION="${condition}"
}

# QAQC MISMATCH ANALYSIS
for file in sql/qaqc_aggregate.sql sql/qaqc_mismatch.sql sql/qaqc_null.sql sql/qaqc_outlier.sql
do
  for mapped in true false
  do
    for condo in true false 
    do
      QAQC ${file} ${mapped} ${condo}
    done
  done 
done 
