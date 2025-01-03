#!/bin/bash
source ../../bash/utils.sh
set_error_traps

rm -rf output

echo "Export product tables"
mkdir -p output && (
    cd output

    echo "Copy metadata files"
    cp ../source_data_versions.csv .
    cp ../build_metadata.json .
    
    echo "export cdbg_block_groups.csv ..."
    csv_export cdbg_block_groups cdbg_block_groups

    echo "export cdbg_tracts.csv ..."
    csv_export cdbg_tracts cdbg_tracts

    echo "export cdbg_boroughs.csv ..."
    csv_export cdbg_boroughs cdbg_boroughs
)

zip -r output/output.zip output
