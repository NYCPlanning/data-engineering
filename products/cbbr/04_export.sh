#!/bin/bash
source config.sh
set_error_traps

echo "CBBR Version ${VERSION} : 04 Export"
OUTPUT_DIRECTORY="output"
mkdir -p $OUTPUT_DIRECTORY
cp ./source_data_versions.csv $OUTPUT_DIRECTORY
cp ./build_metadata.json $OUTPUT_DIRECTORY

echo "Transforming to final schemas ..."
run_sql_file sql/export.sql

cd $OUTPUT_DIRECTORY
echo "Exporting input table to csv file ..."
csv_export omb_cbbr_agency_responses cbbr_submissions_input

echo "Exporting output tables to csv files ..."
csv_export _cbbr_submissions cbbr_submissions_build
csv_export cbbr_export cbbr_export

csv_export cbbr_submissions_needgeoms cbbr_submissions_needgeoms
csv_export cbbr_submissions_needgeoms_a cbbr_submissions_needgeoms_a
csv_export cbbr_submissions_needgeoms_b cbbr_submissions_needgeoms_b
csv_export cbbr_submissions_needgeoms_c cbbr_submissions_needgeoms_c
csv_export cbbr_export_poly cbbr_submissions_poly
csv_export cbbr_export_pts cbbr_submissions_pts

echo "Exporting output geometry tables to zip shapefiles ..."
shp_export cbbr_export_poly MULTIPOLYGON -f cbbr_submissions_poly_shapefile
shp_export cbbr_export_pts MULTIPOINT -f cbbr_submissions_pts_shapefile
echo $VERSION >> version.txt

cd ..

echo "Upload Output to DigitalOcean" 

dcpy lifecycle builds artifacts builds upload -p db-cbbr -a public-read

echo "Done!"
