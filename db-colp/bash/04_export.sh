#!/bin/bash
source ../bash_utils/config.sh
set_env ../.env

rm -rf output
mkdir -p output && (
    cd output

    echo "Exporting COLP"
    csv_export ${BUILD_ENGINE} colp
    csv_export ${BUILD_ENGINE} colp POINT
    fgdb_export colp POINT

    echo "[$(date)] ${DATE}" > version.txt
)

mkdir -p output/qaqc && (
    cd output/qaqc
    
    echo "exporting qaqc"
    csv_export ${BUILD_ENGINE} ipis_unmapped
    csv_export ${BUILD_ENGINE} ipis_colp_geoerrors
    csv_export ${BUILD_ENGINE} ipis_sname_errors
    csv_export ${BUILD_ENGINE} ipis_hnum_errors
    csv_export ${BUILD_ENGINE} ipis_bbl_errors
    csv_export ${BUILD_ENGINE} ipis_cd_errors
    csv_export ${BUILD_ENGINE} ipis_modified_hnums
    csv_export ${BUILD_ENGINE} ipis_modified_names
    csv_export ${BUILD_ENGINE} usetype_changes
    csv_export ${BUILD_ENGINE} modifications_applied
    csv_export ${BUILD_ENGINE} modifications_not_applied
    csv_export ${BUILD_ENGINE} geospatial_check
    csv_export ${BUILD_ENGINE} records_by_agency
    csv_export ${BUILD_ENGINE} records_by_usetype
    csv_export ${BUILD_ENGINE} records_by_agency_usetype

)

zip -r output/output.zip output
