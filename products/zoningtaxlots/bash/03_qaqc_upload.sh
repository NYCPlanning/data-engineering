#!/bin/bash
source bash/config.sh
set_error_traps

echo "Archive final output"
echo "Copy dcp_zoning_taxlot to DB defaultdb ..."
run_sql_command "CREATE TABLE public.dcp_zoning_taxlot AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.dcp_zoning_taxlot;"
pg_dump ${BUILD_ENGINE} -t public.dcp_zoning_taxlot --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.dcp_zoning_taxlot;"

echo "Change defaultdb.public.dcp_zoningtaxlots table's schema to dcp_zoningtaxlots and name to ${VERSION_SQL_TABLE} ..."
psql ${EDM_DATA} -c "
  CREATE SCHEMA IF NOT EXISTS dcp_zoningtaxlots;
  ALTER TABLE dcp_zoning_taxlot SET SCHEMA dcp_zoningtaxlots;
  DROP TABLE IF EXISTS dcp_zoningtaxlots.\"${VERSION_SQL_TABLE}\";
  ALTER TABLE dcp_zoningtaxlots.dcp_zoning_taxlot RENAME TO \"${VERSION_SQL_TABLE}\";
"

echo "Run export and qaqc scripts ..."
run_sql_file sql/export.sql
psql ${EDM_DATA} --set ON_ERROR_STOP=1 -v VERSION=${VERSION_SQL_TABLE} -f sql/qaqc/frequency.sql
psql ${EDM_DATA} --set ON_ERROR_STOP=1 -v VERSION=${VERSION_SQL_TABLE} -v VERSION_PREV=${VERSION_PREV_SQL_TABLE} -f sql/qaqc/bbl.sql
psql ${EDM_DATA} --set ON_ERROR_STOP=1 -v VERSION=${VERSION_SQL_TABLE} -v VERSION_PREV=${VERSION_PREV_SQL_TABLE} -f sql/qaqc/mismatch.sql
psql ${EDM_DATA} --set ON_ERROR_STOP=1 -v VERSION=${VERSION_SQL_TABLE} -v VERSION_PREV=${VERSION_PREV_SQL_TABLE} -f sql/qaqc/out_bbldiffs.sql | 
    psql ${BUILD_ENGINE} --set ON_ERROR_STOP=1 -f sql/qaqc/in_bbldiffs.sql

## remove dtm_id column from archive because it isn't a true id but rather one we generate during build
psql $EDM_DATA -c "ALTER TABLE dcp_zoningtaxlots.\"${VERSION_SQL_TABLE}\" DROP COLUMN dtm_id;"

echo "Generate csvs and shapefiles ..."
rm -rf output && mkdir -p output
(
    cd output

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
    
    csv_export dcp_zoning_taxlot_export zoningtaxlot_db &

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

    shp_export qc_bbldiffs MULTIPOLYGON -t_srs "EPSG:2263"

    echo "${DATE}" > version.txt
    wait
)

psql -q ${EDM_DATA} -v VERSION=${VERSION_SQL_TABLE} -v VERSION_PREV=${VERSION_PREV_SQL_TABLE} \
    -f sql/qaqc/versioncomparison.sql > output/qc_versioncomparison.csv

psql -q ${EDM_DATA} -v VERSION=${VERSION_SQL_TABLE} -v VERSION_PREV=${VERSION_PREV_SQL_TABLE} \
    -f sql/qaqc/null.sql > output/qaqc_null.csv

echo "Upload outpits ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
