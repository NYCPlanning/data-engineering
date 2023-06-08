#!/bin/bash
source ../bash_utils/config.sh
source bash/config.sh
set_env ../.env
set_error_traps

run_sql_file sql/export.sql
psql ${EDM_DATA} -v VERSION=${VERSION} -f sql/qaqc/frequency.sql
psql ${EDM_DATA} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} -f sql/qaqc/bbl.sql
psql ${EDM_DATA} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} -f sql/qaqc/mismatch.sql
psql ${EDM_DATA} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} -f sql/qaqc/out_bbldiffs.sql | 
    psql ${BUILD_ENGINE} -f sql/qaqc/in_bbldiffs.sql

rm -rf output && mkdir -p output
(
    cd output

    csv_export ${BUILD_ENGINE} source_data_versions &
    
    csv_export ${BUILD_ENGINE} dcp_zoning_taxlot_export zoningtaxlot_db &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qaqc_frequency 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_frequency.csv &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qaqc_bbl 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_bbl.csv &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qaqc_mismatch 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_mismatch.csv &

    run_sql_command "\copy (
    SELECT boroughcode, taxblock,taxlot , bblnew ,zd1new , 
        zd2new ,zd3new , zd4new ,co1new , co2new ,
        sd1new , sd2new ,sd3new , lhdnew zmnnew , 
        zmcnew , area, inzonechange , 
        bblprev, zd1prev, zd2prev, zd3prev, zd4prev, 
        co1prev, co2prev, sd1prev, sd2prev, sd3prev, 
        lhdprev, zmnprev, zmcprev
    FROM qc_bbldiffs
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qc_bbldiffs.csv &

    shp_export qc_bbldiffs MULTIPOLYGON

    echo "$DATE" > version.txt
    wait
)

psql -q ${EDM_DATA} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} \
    -f sql/qaqc/versioncomparison.sql > output/qc_versioncomparison.csv

psql -q ${EDM_DATA} -v VERSION=${VERSION} -v VERSION_PREV=${VERSION_PREV} \
    -f sql/qaqc/null.sql > output/qaqc_null.csv

Upload db-zoningtaxlots $DATE
Upload db-zoningtaxlots latest