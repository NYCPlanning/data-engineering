#!/bin/bash
source bash/config.sh

echo "Run build"

run_sql_file sql/create_priority.sql &
run_sql_file sql/create.sql
run_sql_file sql/preprocessing.sql
run_sql_file sql/bbl.sql

wait
run_sql_file sql/area_zoningdistrict_create.sql &
run_sql_file sql/area_commercialoverlay.sql &
run_sql_file sql/area_specialdistrict.sql &
run_sql_file sql/area_limitedheight.sql &
run_sql_file sql/area_zoningmap.sql

wait
run_sql_file sql/area_zoningdistrict.sql
run_sql_file sql/parks.sql
run_sql_file sql/inzonechange.sql
run_sql_file sql/correct_duplicatevalues.sql
run_sql_file sql/correct_zoninggaps.sql
run_sql_file sql/correct_invalidrecords.sql


echo "Archive final output"

pg_dump -d ${BUILD_ENGINE} -t dcp_zoning_taxlot --no-owner --clean | psql ${EDM_DATA}
psql ${EDM_DATA} -c "
  CREATE SCHEMA IF NOT EXISTS dcp_zoningtaxlots;
  ALTER TABLE dcp_zoning_taxlot SET SCHEMA dcp_zoningtaxlots;
  DROP TABLE IF EXISTS dcp_zoningtaxlots.\"${VERSION_SQL_TABLE}\";
  ALTER TABLE dcp_zoningtaxlots.dcp_zoning_taxlot RENAME TO \"${VERSION_SQL_TABLE}\";
"
