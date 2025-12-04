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
