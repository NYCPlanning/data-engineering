#!/bin/bash
source bash/config.sh

echo "Run build"

echo "Build section 1 ..."
run_sql_file sql/create_priority.sql
run_sql_file sql/create.sql
run_sql_file sql/preprocessing.sql
run_sql_file sql/bbl.sql
wait

echo "Build section 2 ..."
run_sql_file sql/area_commercialoverlay.sql
run_sql_file sql/area_specialdistrict.sql
run_sql_file sql/area_limitedheight.sql
run_sql_file sql/area_zoningmap.sql
wait

echo "Build section 3 ..."
run_sql_file sql/area_zoningdistrict.sql
run_sql_file sql/parks.sql
run_sql_file sql/inzonechange.sql
run_sql_file sql/correct_duplicatevalues.sql
run_sql_file sql/correct_zoninggaps.sql
run_sql_file sql/correct_invalidrecords.sql
