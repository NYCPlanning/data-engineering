#!/bin/bash
source bash/config.sh
set_error_traps

echo "Generate csvs and shapefiles ..."
rm -rf output && mkdir -p output
(
    cd output

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
    
    csv_export zoningtaxlots zoningtaxlot_db &

    run_sql_command "\copy (
    SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_freq 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_frequency.csv &

    run_sql_command "\copy (
    SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_bbl 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_bbl.csv &

    run_sql_command "\copy (
    SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_mismatch 
    order by version::timestamp
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_mismatch.csv &

    run_sql_command "\copy (
    SELECT borough_code, tax_block,tax_lot , bblnew ,zd1new , 
        zd2new ,zd3new , zd4new ,co1new , co2new ,
        sd1new , sd2new ,sd3new , lhdnew zmnnew , 
        zmcnew , area, inzonechange , 
        bblprev, zd1prev, zd2prev, zd3prev, zd4prev, 
        co1prev, co2prev, sd1prev, sd2prev, sd3prev, 
        lhdprev, zmnprev, zmcprev
    FROM ${BUILD_ENGINE_SCHEMA}.qa_bbldiffs
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qc_bbldiffs.csv &

    shp_export qa_bbldiffs MULTIPOLYGON -t_srs "EPSG:2263"

    run_sql_command "\copy (
    SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_vers_comparison 
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qc_versioncomparison.csv &

    run_sql_command "\copy (
    SELECT * FROM ${BUILD_ENGINE_SCHEMA}.qa_null 
    ) TO STDOUT DELIMITER ',' CSV HEADER;" > qaqc_null.csv

    echo "${DATE}" > version.txt
    wait
)

echo "Upload outputs ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
