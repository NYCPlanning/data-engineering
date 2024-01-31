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

echo "Generate output tables"
run_sql_command "ALTER TABLE _kpdb RENAME COLUMN geom TO geometry;"
run_sql_file sql/product/kpdb.sql
