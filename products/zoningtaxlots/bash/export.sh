#!/bin/bash
source ../../bash/utils.sh
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
    csv_export qa_bbldiffs qc_bbldiffs &

    shp_export qa_bbldiffs MULTIPOLYGON -t_srs "EPSG:2263"

    echo "${VERSION}" > version.txt
    wait
)

echo "Upload outputs ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
