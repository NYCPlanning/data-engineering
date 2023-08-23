#!/bin/bash

rm -rf output
mkdir -p output && (
    cd output

    echo "Exporting COLP"
    csv_export colp
    shp_export colp POINT
    fgdb_export colp POINT

    echo "[$(date)] ${DATE}" > version.txt
)

mkdir -p output/qaqc && (
    cd output/qaqc
    
    echo "exporting qaqc"
    csv_export ipis_unmapped
    csv_export ipis_colp_geoerrors
    csv_export ipis_sname_errors
    csv_export ipis_hnum_errors
    csv_export ipis_bbl_errors
    csv_export ipis_cd_errors
    csv_export ipis_modified_hnums
    csv_export ipis_modified_names
    csv_export usetype_changes
    csv_export modifications_applied
    csv_export modifications_not_applied
    csv_export geospatial_check
    csv_export records_by_agency
    csv_export records_by_usetype
    csv_export records_by_agency_usetype

)

zip -r output/output.zip output
