#!/bin/bash
# was run using db-data-library dev container
# https://gdal.org/programs/ogr2ogr.html
set -e
echo "Hi! Reprojecting DTM shapefile..."

# ogr2ogr -f "SQlite" -dsco spatialite=yes output.sqlite dof_dtm_0/dof_dtm_tax_lot_polygon.shp -nlt GEOMETRY
ogr2ogr -makevalid -nlt MULTIPOLYGON -s_srs EPSG:2263 -t_srs EPSG:4326 dof_dtm_1.shp dof_dtm_0/dof_dtm_tax_lot_polygon.shp

echo "Done"
