#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Generate output tables"
run_sql_file sql/_export.sql

echo "Archive tables"
archive public.kpdb kpdb.kpdb &
archive public.combined kpdb.combined &
archive public.review_dob kpdb.review_dob &
archive public.review_project kpdb.review_project
archive public.corrections_applied kpdb.corrections_applied &
archive public.corrections_not_applied kpdb.corrections_not_applied &
archive public.corrections_zap kpdb.corrections_zap &
archive public.corrections_dob_match kpdb.corrections_dob_match &
archive public.corrections_project kpdb.corrections_project &
archive public.corrections_main kpdb.corrections_main &
wait

rm -rf output
mkdir -p output
(
    cd output

    echo "Exporting review tables"
    mkdir -p review

    (
        cd review
        shp_export combined MULTIPOLYGON &
        shp_export review_project MULTIPOLYGON &
        shp_export review_dob MULTIPOLYGON 
        wait

        run_sql_command "ALTER TABLE review_project DROP COLUMN geom;" &
        run_sql_command "ALTER TABLE review_dob DROP COLUMN geom;" &
        run_sql_command "ALTER TABLE combined DROP COLUMN geom;"
        wait 
        
        csv_export combined &
        csv_export review_project &
        csv_export review_dob &
        csv_export corrections_applied &
        csv_export corrections_not_applied &
        csv_export corrections_zap &
        csv_export corrections_dob_match &
        csv_export corrections_project &
        csv_export corrections_main
        wait
        
        echo "Compress large csvs"
        zip -9 combined.zip combined.csv
        rm combined.csv
        zip -9 review_dob.zip review_dob.csv
        rm review_dob.csv
        zip -9 review_project.zip review_project.csv
        rm review_project.csv
    )
    echo "Compress review folder"
    zip -r review.zip review/

    mkdir -p sca
    echo "Export SCA aggregate tables"
    (
        cd sca
        csv_export longform_csd_output &
        csv_export longform_es_zone_output &
        csv_export longform_subdist_output_cp_assumptions
        wait
    )
    echo "Compress SCA folder"
    zip -r sca.zip sca/
    rm -rf sca

    echo "Exporting output tables"
    csv_export kpdb
    Compress kpdb.csv
    shp_export kpdb MULTIPOLYGON
)

# Upload
SENDER=${1:-unknown}
python3 -m python.upload $SENDER
