#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Run build"
# Create functions and procedures
run_sql_file sql/_functions.sql
run_sql_file sql/_procedures.sql

# Map source data
run_sql_file sql/dcp_application.sql
run_sql_file sql/dcp_housing.sql
run_sql_file sql/combine.sql
run_sql_command "CALL apply_correction('${BUILD_ENGINE_SCHEMA}', 'combined', 'corrections_main');"
run_sql_command "VACUUM ANALYZE combined;"

# Find and matches between non-DOB sources
run_sql_file sql/_project_record_ids.sql

# Apply corrections to reassign records to projects
run_sql_file sql/correct_projects.sql
run_sql_command "VACUUM ANALYZE _project_record_ids;"

# Output corrected projects for review
run_sql_file sql/review_project.sql

# Find matches between DOB and non-DOB sources
run_sql_file sql/review_dob.sql

# Create project IDs and deduplicate units
run_sql_file sql/project_record_ids.sql 
run_sql_command "VACUUM ANALYZE project_record_ids;"

# Dedup units
python3 -m python.dedup_units
run_sql_command "CALL apply_correction('${BUILD_ENGINE_SCHEMA}', 'deduped_units', 'corrections_main');"
run_sql_command "VACUUM ANALYZE deduped_units;"

# Join to boro, clean duplicates
run_sql_file sql/join_boroughs.sql
run_sql_command "CALL apply_correction('${BUILD_ENGINE_SCHEMA}', 'combined', 'corrections_borough');"

# Create KPDB
run_sql_file sql/create_kpdb.sql

# Prepare output for export
echo "Generate output tables"
run_sql_file sql/_export.sql

rm -rf output
mkdir -p output
(
    cd output

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

    # TODO fix SCA aggregation
    # mkdir -p sca
    # echo "Export SCA aggregate tables"
    # (
    #     cd sca
    #     csv_export longform_csd_output &
    #     csv_export longform_es_zone_output &
    #     csv_export longform_subdist_output_cp_assumptions
    #     wait
    # )
    # echo "Compress SCA folder"
    # zip -r sca.zip sca/
    # rm -rf sca

    echo "Exporting output tables"
    csv_export kpdb
    zip -9 kpdb.zip kpdb.csv
    rm kpdb.csv
    shp_export kpdb MULTIPOLYGON
)
