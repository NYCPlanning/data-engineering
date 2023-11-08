#!/bin/bash
source ../../bash/utils.sh
set_error_traps

# Prepare output for export
echo "Generate output tables"
run_sql_file sql/_export.sql

rm -rf output
mkdir -p output
(
    cd output

    csv_export source_data_versions

    echo "Exporting review tables"
    mkdir -p review

    (
        cd review
        shp_export combined MULTIPOLYGON
        shp_export review_project MULTIPOLYGON
        shp_export review_dob MULTIPOLYGON

        run_sql_command "ALTER TABLE review_project DROP COLUMN geom;" &
        run_sql_command "ALTER TABLE review_dob DROP COLUMN geom;" &
        run_sql_command "ALTER TABLE combined DROP COLUMN geom;"
        wait 
        
        csv_export combined &
        csv_export review_project &
        csv_export review_dob &
        csv_export corrections_applied &
        csv_export corrections_not_applied &
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

    echo "Export SCA aggregation tables"
    mkdir -p sca_aggregation
    (
        cd sca_aggregation
        csv_export longform_csd_output &
        csv_export longform_es_zone_output &
        csv_export longform_subdist_output_cp_assumptions
        wait
    )
    echo "Compress SCA folder"
    zip -r sca_aggregation.zip sca_aggregation/
    rm -rf sca_aggregation

    echo "Export aggregation tables"
    mkdir -p aggregation
    (
        cd aggregation
        csv_export longform_ct_output
        csv_export longform_nta_output
        csv_export longform_cdta_output
        csv_export longform_cd_output
    )
    echo "Compress aggregation folder"
    zip -r aggregation.zip aggregation/
    rm -rf aggregation

    echo "Exporting output tables"
    csv_export kpdb
    zip -9 kpdb.zip kpdb.csv
    rm kpdb.csv
    shp_export kpdb MULTIPOLYGON
)
