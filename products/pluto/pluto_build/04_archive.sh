#!/bin/bash
source ./bash/config.sh
set_error_traps

echo 'Create Export'
run_sql_file sql/export.sql

run_sql_file \
  sql/export_mappluto_shp.sql\
  -v TABLE='mappluto'\
  -v GEOM='clipped_2263'

run_sql_file \
  sql/export_mappluto_shp.sql\
  -v TABLE='mappluto_unclipped'\
  -v GEOM='geom_2263'

run_sql_file \
  sql/export_mappluto_gdb.sql\
  -v TABLE='mappluto_gdb'\
  -v GEOM='clipped_2263'

run_sql_file \
  sql/export_mappluto_gdb.sql\
  -v TABLE='mappluto_unclipped_gdb'\
  -v GEOM='geom_2263'

run_sql_command "
  DROP TABLE IF EXISTS mappluto_sample;
  SELECT * INTO mappluto_sample FROM mappluto_unclipped_gdb limit 5;
  ALTER TABLE mappluto_sample ALTER COLUMN \"Borough\" SET NOT NULL;
  ALTER TABLE mappluto_sample ALTER COLUMN \"Block\" SET NOT NULL;
  ALTER TABLE mappluto_sample ALTER COLUMN \"Lot\" SET NOT NULL;
  ALTER TABLE mappluto_sample ALTER COLUMN \"BBL\" SET NOT NULL;
  ALTER TABLE mappluto_sample ALTER COLUMN \"BoroCode\" SET NOT NULL;"

pg_dump -t archive_pluto ${BUILD_ENGINE} -O -c --if-exists | psql ${EDM_DATA}
psql ${EDM_DATA} -c "
    CREATE SCHEMA IF NOT EXISTS dcp_pluto;
    ALTER TABLE archive_pluto SET SCHEMA dcp_pluto;
    DROP TABLE IF EXISTS dcp_pluto.\"${VERSION}\";
    ALTER TABLE dcp_pluto.archive_pluto RENAME TO \"${VERSION}\";";

