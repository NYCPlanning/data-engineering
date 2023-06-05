#!/bin/bash
source ../../bash_utils/config.sh
set_env ../../.env
set_env version.env

echo "CBBR Version ${VERSION} : 04 Export"
OUTPUT_DIRECTORY="output/${VERSION}"
mkdir -p ${OUTPUT_DIRECTORY}

echo "Transforming to final schemas ..."
run_sql_file sql/export.sql

cd ${OUTPUT_DIRECTORY}
echo "Exporting input table to csv file ..."
csv_export ${BUILD_ENGINE} cbbr_submissions cbbr_submissions_input

echo "Exporting output tables to csv files ..."
csv_export ${BUILD_ENGINE} _cbbr_submissions cbbr_submissions_build
csv_export ${BUILD_ENGINE} cbbr_export cbbr_export

csv_export ${BUILD_ENGINE} cbbr_submissions_needgeoms cbbr_submissions_needgeoms
csv_export ${BUILD_ENGINE} cbbr_submissions_needgeoms_a cbbr_submissions_needgeoms_a
csv_export ${BUILD_ENGINE} cbbr_submissions_needgeoms_b cbbr_submissions_needgeoms_b
csv_export ${BUILD_ENGINE} cbbr_submissions_needgeoms_c cbbr_submissions_needgeoms_c
csv_export ${BUILD_ENGINE} cbbr_export_poly cbbr_submissions_poly
csv_export ${BUILD_ENGINE} cbbr_export_pts cbbr_submissions_pts

echo "Exporting output geometry tables to zip shapefiles ..."
shp_export cbbr_export_poly MULTIPOLYGON cbbr_submissions_poly_shapefile
shp_export cbbr_export_pts MULTIPOINT cbbr_submissions_pts_shapefile

echo "Upload Output to DigitalOcean" 

wait
upload db-cbbr $(date "+%Y-%m-%d")
upload db-cbbr "latest"
wait
exit 0

echo "Done!"
