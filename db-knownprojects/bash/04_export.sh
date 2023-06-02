#!/bin/bash
set -e
source bash/config.sh

echo "Generate output tables"
psql $BUILD_ENGINE -f sql/_export.sql

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
        SHP_export combined MULTIPOLYGON &
        SHP_export review_project MULTIPOLYGON &
        SHP_export review_dob MULTIPOLYGON 
        wait

        psql $BUILD_ENGINE  -c "ALTER TABLE review_project DROP COLUMN geom;" &
        psql $BUILD_ENGINE  -c "ALTER TABLE review_dob DROP COLUMN geom;" &
        psql $BUILD_ENGINE  -c "ALTER TABLE combined DROP COLUMN geom;"
        wait 
        
        CSV_export combined &
        CSV_export review_project &
        CSV_export review_dob &
        CSV_export corrections_applied &
        CSV_export corrections_not_applied &
        CSV_export corrections_zap &
        CSV_export corrections_dob_match &
        CSV_export corrections_project &
        CSV_export corrections_main
        wait
        
        Compress combined.csv
        Compress review_dob.csv
        Compress review_project.csv
    )
    echo "Compress review folder"
    zip -r review.zip review/

    mkdir -p sca
    echo "Export SCA aggregate tables"
    (
        cd sca
        CSV_export longform_csd_output &
        CSV_export longform_es_zone_output &
        CSV_export longform_subdist_output_cp_assumptions
        wait
    )
    echo "Compress SCA folder"
    zip -r sca.zip sca/
    rm -rf sca

    echo "Exporting output tables"
    CSV_export kpdb
    Compress kpdb.csv
    SHP_export kpdb MULTIPOLYGON
)

# Upload
SENDER=${1:-unknown}
python3 -m python.upload $SENDER
