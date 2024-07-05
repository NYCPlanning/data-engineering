#!/bin/bash
source ./ae_output_scripts/config.sh
set_error_traps

echo "Generating output tables ..."
run_sql_file sql/export_ae_tables.sql

mkdir -p ae_output && (
    cd ae_output

    echo "Generating output files ..."
    mkdir -p tables && (
        cd tables
        csv_export ae_zoning_district zoning_district
    )
)
