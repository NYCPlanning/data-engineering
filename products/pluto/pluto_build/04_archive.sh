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
