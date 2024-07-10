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
    csv_export qaqc_frequency qaqc_frequency &
    csv_export qaqc_bbl qaqc_bbl &
    csv_export qaqc_mismatch qaqc_mismatch &
    csv_export qc_versioncomparison qc_versioncomparison &
    csv_export qaqc_null qaqc_null &
    csv_export qc_bbldiffs qc_bbldiffs &

    shp_export qc_bbldiffs MULTIPOLYGON -f qc_bbldiffs -t_srs "EPSG:2263"

    echo "${VERSION}" > version.txt
    wait
)

echo "Upload outputs ..."
python3 -m dcpy.connectors.edm.publishing upload -p db-zoningtaxlots -a public-read
