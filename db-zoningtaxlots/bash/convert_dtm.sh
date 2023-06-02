#!/bin/bash
# was run using db-knownprojects-data dev container
# https://manpages.debian.org/experimental/postgis/shp2pgsql.1.en.html
# http://www.bostongis.com/pgsql2shp_shp2pgsql_quickguide_20.bqg
set -e
echo "Hi! Convert DTM shapefile to PostgreSQL..."

# ogr2ogr dof_dtm_2/dof_dtm_2.sql dof_dtm_1/dof_dtm_1.shp
shp2pgsql -d -D -I -g wkb_geometry -s 4326 dof_dtm_1/dof_dtm_1.shp public.dof_dtm >dof_dtm_2/dof_dtm_2.sql

echo "Done"
