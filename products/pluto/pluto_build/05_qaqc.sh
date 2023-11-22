#!/bin/bash
source ./bash/config.sh
set_error_traps

import_qaqc qaqc_expected $VERSION_PREV
import_qaqc qaqc_aggregate $VERSION_PREV
import_qaqc qaqc_mismatch $VERSION_PREV
import_qaqc qaqc_null $VERSION_PREV
import_qaqc qaqc_outlier $VERSION_PREV

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
    sqlfile=sql/${1}.sql
    echo "Running ${sqlfile}"
    mapped=${2}
    condo=${3}
    set_condition ${mapped} ${condo}
    run_sql_file ${sqlfile} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} -v CONDO=${condo} \
    -v MAPPED=${mapped}  -v CONDITION="${condition}"
}

# QAQC MISMATCH ANALYSIS
for file in qaqc_aggregate qaqc_mismatch qaqc_null qaqc_outlier; do
    for mapped in true false; do
        for condo in true false; do
            QAQC ${file} ${mapped} ${condo}
        done
    done 
done 
