#!/bin/bash
source config.sh

echo "CBBR Version $VERSION : 04 Export"
OUTPUT_DIRECTORY="output/$VERSION"
mkdir -p $OUTPUT_DIRECTORY

echo "Transforming to final schemas ..."
run_sql sql/export.sql

cd $OUTPUT_DIRECTORY
echo "Exporting input table to csv file ..."
CSV_export $BUILD_ENGINE cbbr_submissions cbbr_submissions_input

echo "Exporting output tables to csv files ..."
CSV_export $BUILD_ENGINE _cbbr_submissions cbbr_submissions_build
CSV_export $BUILD_ENGINE cbbr_export cbbr_export

CSV_export $BUILD_ENGINE cbbr_submissions_needgeoms cbbr_submissions_needgeoms
CSV_export $BUILD_ENGINE cbbr_submissions_needgeoms_a cbbr_submissions_needgeoms_a
CSV_export $BUILD_ENGINE cbbr_submissions_needgeoms_b cbbr_submissions_needgeoms_b
CSV_export $BUILD_ENGINE cbbr_submissions_needgeoms_c cbbr_submissions_needgeoms_c
CSV_export $BUILD_ENGINE cbbr_export_poly cbbr_submissions_poly
CSV_export $BUILD_ENGINE cbbr_export_pts cbbr_submissions_pts

echo "Exporting output geometry tables to zip shapefiles ..."
SHP_export $BUILD_ENGINE cbbr_export_poly MULTIPOLYGON cbbr_submissions_poly_shapefile
SHP_export $BUILD_ENGINE cbbr_export_pts MULTIPOINT cbbr_submissions_pts_shapefile

echo "Upload Output to DigitalOcean" 

wait
Upload 
wait
exit 0

echo "Done!"
