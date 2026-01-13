#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Generate csvs and shapefiles ..."
rm -rf output && mkdir -p output
(
    cd output

    cp ../source_data_versions.csv ./
    cp ../build_metadata.json ./
    
    csv_export zoningtaxlot_db &
    csv_export qaqc_frequency &
    csv_export qaqc_bbl &
    csv_export qaqc_mismatch &
    csv_export qc_versioncomparison &
    csv_export qaqc_null &
    csv_export qc_bbldiffs &

    shp_export qc_bbldiffs MULTIPOLYGON -t_srs "EPSG:2263"

    echo "${VERSION}" > version.txt
    wait
)

echo "Upload outputs ..."
dcpy lifecycle builds artifacts builds upload -p db-zoningtaxlots -a public-read
