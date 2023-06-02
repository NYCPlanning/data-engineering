#!/bin/bash
source bash/config.sh

display "Generate output tables"
psql $BUILD_ENGINE\
    -v VERSION=$VERSION\
    -v CAPTURE_DATE=$CAPTURE_DATE\
    -f sql/_export.sql

mkdir -p output 
(
    cd output

    display "Export Devdb and HousingDB"
    CSV_export EXPORT_housing housing &
    SHP_export SHP_housing housing &

    CSV_export EXPORT_devdb devdb &
    SHP_export SHP_devdb devdb &
    
    display "Export HNY_devdb_lookup"
    CSV_export HNY_devdb_lookup &

    display "Export QAQC Tables"
    CSV_export FINAL_qaqc &
    CSV_export HNY_no_match & 
    CSV_export qaqc_app &
    CSV_export qaqc_historic
    pg_dump -d $BUILD_ENGINE -t qaqc_historic -f qaqc_historic.sql
    CSV_export qaqc_field_distribution &
    CSV_export qaqc_quarter_check &
    
    display "Export Corrections"
    CSV_export CORR_hny_matches &
    CSV_export corrections_applied &
    CSV_export corrections_not_applied &
    CSV_export corrections_reference &
    CSV_export _manual_corrections manual_corrections &

    display "Export A2 units for review by housing"
    CSV_export EXPORT_A2_devdb

    wait
    display "CSV Export Complete"
    echo "[$(date)] $VERSION" > version.txt
)

zip -r output/output.zip output

wait 
display "Upload Complete"