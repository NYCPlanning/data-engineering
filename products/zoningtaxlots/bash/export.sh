#!/bin/bash
source bash/config.sh
set_error_traps

echo "BUILD ENGINE SCHEMA: ${BUILD_ENGINE_SCHEMA}" 
echo "BUILD ENGINE: ${BUILD_ENGINE}"
echo "EDM: ${EDM_DATA}"
echo "Archive final output"
echo "Copy int__zoningtaxlots to DB defaultdb ..."
run_sql_command "CREATE TABLE public.dcp_zoning_taxlot AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.int__zoningtaxlots;"
pg_dump ${BUILD_ENGINE} -t public.dcp_zoning_taxlot --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.dcp_zoning_taxlot;"

echo "Change defaultdb.public.dcp_zoningtaxlots table's schema to dcp_zoningtaxlots and name to ${VERSION} ..."
psql ${EDM_DATA} -c "
  CREATE SCHEMA IF NOT EXISTS dcp_zoningtaxlots;
  ALTER TABLE dcp_zoning_taxlot SET SCHEMA dcp_zoningtaxlots;
  DROP TABLE IF EXISTS dcp_zoningtaxlots.\"${VERSION}\";
  ALTER TABLE dcp_zoningtaxlots.dcp_zoning_taxlot RENAME TO \"${VERSION}\";
"

echo "Archive export and qaqc tables ..."

echo "Copy zoningtaxlots to DB defaultdb ..."
run_sql_command "CREATE TABLE public.zoningtaxlots AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.zoningtaxlots;"
pg_dump ${BUILD_ENGINE} -t public.zoningtaxlots --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.zoningtaxlots;"

echo "Copy qaqc_freq to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qaqc_freq AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_freq;"
pg_dump ${BUILD_ENGINE} -t public.qaqc_freq --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qaqc_freq;"

echo "Copy qa_bbl to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qa_bbl AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_bbl;"
pg_dump ${BUILD_ENGINE} -t public.qa_bbl --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qa_bbl;"

echo "Copy qa_mismatch to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qa_mismatch AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_mismatch;"
pg_dump ${BUILD_ENGINE} -t public.qa_mismatch --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qa_mismatch;"

echo "Copy qa_bbldiffs to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qa_bbldiffs AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_bbldiffs;"
pg_dump ${BUILD_ENGINE} -t public.qa_bbldiffs --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qa_bbldiffs;"

## remove dtm_id column from archive because it isn't a true id but rather one we generate during build
psql $EDM_DATA -c "ALTER TABLE dcp_zoningtaxlots.\"${VERSION}\" DROP COLUMN dtm_id;"

echo "Generate csvs and shapefiles ..."
rm -rf output && mkdir -p output
(
    cd output

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
    
    csv_export zoningtaxlots zoningtaxlot_db &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qaqc_freq 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_frequency.csv &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qa_bbl 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_bbl.csv &

    psql ${EDM_DATA} -c "\copy (
    SELECT * FROM dcp_zoningtaxlots.qa_mismatch 
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
    FROM qa_bbldiffs
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qa_bbldiffs.csv &

    shp_export qa_bbldiffs MULTIPOLYGON -t_srs "EPSG:2263"

    echo "${DATE}" > version.txt
    wait
)

echo "Copy qa_vers_comparison to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qa_vers_comparison AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_vers_comparison;"
pg_dump ${BUILD_ENGINE} -t public.qa_vers_comparison --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qa_vers_comparison;"


echo "Copy qa_null to DB defaultdb ..."
run_sql_command "CREATE TABLE public.qa_null AS SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_null;"
pg_dump ${BUILD_ENGINE} -t public.qa_null --no-owner --clean | psql ${EDM_DATA}
run_sql_command "DROP TABLE IF EXISTS public.qa_null;"

echo "Upload outpits ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
