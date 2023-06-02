#!/bin/bash
source bin/config.sh

# import previous version of pluto
import_public dcp_pluto $VERSION_PREV 
psql $BUILD_ENGINE -c "ALTER TABLE IF EXISTS dcp_pluto RENAME to previous_pluto"

# Download Existing QAQC from DO
import_qaqc qaqc_expected main &
import_qaqc qaqc_aggregate main &
import_qaqc qaqc_mismatch main &
import_qaqc qaqc_null main &
import_qaqc qaqc_outlier main &

wait

# QAQC EXPECTED VALUE ANALYSIS
psql $BUILD_ENGINE \
  -v VERSION=$VERSION \
  -f sql/qaqc_expected.sql &

function set_condition {
  mapped=$1 
  condo=$2
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
  echo "Running $1"
  file=$1
  mapped=$2
  condo=$3
  set_condition $mapped $condo
  psql $BUILD_ENGINE -v VERSION=$VERSION -v VERSION_PREV=$VERSION_PREV -v CONDO=$condo \
  -v MAPPED=$mapped  -v CONDITION="$condition" -f $file
}

# QAQC MISMATCH ANALYSIS
for file in sql/qaqc_aggregate.sql sql/qaqc_mismatch.sql sql/qaqc_null.sql sql/qaqc_outlier.sql
do
  for mapped in true false
  do
    for condo in true false 
    do
      QAQC $file $mapped $condo
    done
  done 
done 
