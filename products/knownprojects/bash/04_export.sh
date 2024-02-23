#!/bin/bash
source ../../bash/utils.sh
set_error_traps

rm -rf output
mkdir -p output
(
    cd output
    csv_export source_data_versions

    echo "Export review tables"
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
        csv_export review_no_geometry &
        csv_export corrections_applied &
        csv_export corrections_not_applied &
        csv_export corrections_dob_match &
        csv_export corrections_project &
        csv_export corrections_main
        wait
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
    echo "Compress SCA aggregation folder"
    zip -r sca_aggregation.zip sca_aggregation/

    echo "Export aggregation tables"
    mkdir -p aggregation
    (
        cd aggregation
        csv_export longform_ct_output
        csv_export longform_nta_output
        csv_export longform_cdta_output
        csv_export longform_cd_output
        wait
    )
    echo "Compress aggregation folder"
    zip -r aggregation.zip aggregation/

    echo "Export summary tables"
    mkdir -p summary
    (   
        cd summary
        csv_export summary_record_phasing
        wait
    )

    echo "Exporting primary product table"
    csv_export kpdb
    shp_export kpdb MULTIPOLYGON
)
