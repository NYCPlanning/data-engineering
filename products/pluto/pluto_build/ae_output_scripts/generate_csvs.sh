#!/bin/bash
source ./ae_output_scripts/config.sh
set_error_traps

run_sql_file sql/export_ae_tables.sql
run_sql_file sql/export_ae_tilesets.sql

mkdir -p ae_output && (
    cd ae_output

    echo "Generating output tables"
    mkdir -p tables && (
        cd tables
        csv_export ae_tax_lot tax_lot
        csv_export ae_zoning_district zoning_district
        csv_export ae_zoning_district_zoning_district_class zoning_district_zoning_district_class
        csv_export ae_zoning_district_class zoning_district_class
    )
)
