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

    csv_export qa_freq qaqc_frequency &

    csv_export qa_bbl qaqc_bbl &

    csv_export qa_mismatch qaqc_mismatch &

    csv_export qa_freq qaqc_bbl &

    csv_export qa_freq qaqc_bbl &

    csv_export qa_vers_comparison qc_versioncomparison &

    csv_export qa_null qaqc_null &

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

    echo "${DATE}" > version.txt
    wait
)

echo "Upload outputs ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
