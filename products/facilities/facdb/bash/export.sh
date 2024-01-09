#!/bin/bash

FILE_DIR=$(dirname "$(readlink -f "$0")")
source $FILE_DIR/../../../../bash/utils.sh
set_error_traps

max_bg_procs 5

mkdir -p output && (
    cd output
    cp ../facdb/metadata.yml ./
    cp ../build_metadata.json ./
    cp ../source_data_versions.csv ./
    csv_export facdb_without_geom_col facilities &
    csv_export qc_operator &
    csv_export qc_oversight &
    csv_export qc_classification &
    csv_export qc_captype &
    csv_export qc_mapped &
    csv_export qc_diff &
    csv_export qc_recordcounts &
    csv_export qc_subgrpbins &
    # csv_export geo_rejects &
    # csv_export geo_result &
    shp_export facdb POINT -f facilities
    fgdb_export facdb POINT facilities
    wait
    echo "export complete"
)
