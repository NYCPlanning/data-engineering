#!/bin/bash
CURRENT_DIR=$(dirname "$(readlink -f "$0")")
source $CURRENT_DIR/config.sh
max_bg_procs 5

mkdir -p output && (
    cd output
    mv ../facdb/metadata.yml .
    echo "*" > .gitignore
    CSV_export facdb facilities &
    CSV_export qc_operator &
    CSV_export qc_oversight &
    CSV_export qc_classification &
    CSV_export qc_captype &
    CSV_export qc_mapped &
    CSV_export qc_diff &
    CSV_export qc_recordcounts &
    CSV_export qc_subgrpbins &
    # CSV_export geo_rejects &
    # CSV_export geo_result &
    SHP_export facdb POINT facilities
    FGDB_export facdb POINT facilities
    wait
    echo "export complete"
)
