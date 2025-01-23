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
    csv_export cdbg_block_groups
    
    echo "export cdbg_block_groups_excel.csv ..."
    csv_export cdbg_block_groups_excel

    echo "export cdbg_tracts.csv ..."
    csv_export cdbg_tracts

    echo "export cdbg_tracts_excel.csv ..."
    csv_export cdbg_tracts_excel

    echo "export cdbg_tracts_geosupport.csv ..."
    csv_export cdbg_tracts_geosupport

    echo "export cdbg_boroughs.csv ..."
    csv_export cdbg_boroughs

    echo "export cdbg_boroughs_excel.csv ..."
    csv_export cdbg_boroughs_excel

    echo "export cdbg_zap_eligibility.csv ..."
    csv_export cdbg_zap_eligibility

    echo "export cdbg_zap_eligibility_excel.csv ..."
    csv_export cdbg_zap_eligibility_excel
)

zip -r output/output.zip output
