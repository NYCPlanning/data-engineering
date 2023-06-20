#!/bin/bash
source bash/config.sh

display "Generate output tables"
run_sql_file sql/_export.sql\
    -v VERSION=$VERSION\
    -v CAPTURE_DATE=$CAPTURE_DATE

mkdir -p output 
(
    cd output

    display "Export Devdb and HousingDB"
    csv_export $BUILD_ENGINE EXPORT_housing housing &
    shp_export SHP_housing POINT housing &

    csv_export $BUILD_ENGINE EXPORT_devdb devdb &
    shp_export SHP_devdb POINT devdb &
    
    display "Export HNY_devdb_lookup"
    csv_export $BUILD_ENGINE HNY_devdb_lookup &

    display "Export QAQC Tables"
    csv_export $BUILD_ENGINE FINAL_qaqc &
    csv_export $BUILD_ENGINE HNY_no_match & 
    csv_export $BUILD_ENGINE qaqc_app &
    csv_export $BUILD_ENGINE qaqc_historic
    pg_dump -d $BUILD_ENGINE -t qaqc_historic -f qaqc_historic.sql
    csv_export $BUILD_ENGINE qaqc_field_distribution &
    csv_export $BUILD_ENGINE qaqc_quarter_check &
    
    display "Export Corrections"
    csv_export $BUILD_ENGINE CORR_hny_matches &
    csv_export $BUILD_ENGINE corrections_applied &
    csv_export $BUILD_ENGINE corrections_not_applied &
    csv_export $BUILD_ENGINE corrections_reference &
    csv_export $BUILD_ENGINE _manual_corrections manual_corrections &

    display "Export A2 units for review by housing"
    csv_export $BUILD_ENGINE EXPORT_A2_devdb

    display "Export source data versions"
    csv_export $BUILD_ENGINE source_data_versions
    wait

    display "CSV Export Complete"
    echo "[$(date)] ${VERSION}" > version.txt
)

zip -r output/output.zip output

wait 
display "Upload Complete"