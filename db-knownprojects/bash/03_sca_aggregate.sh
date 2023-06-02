#!/bin/bash
source bash/config.sh

echo "Create the longfrom SCA Aggregate Tables..."

echo "Preprocess column names to standardize"
#Preprocess the tables to standardize geometry column name 
psql $BUILD_ENGINE -1 -f sca_aggregate/preprocessing.sql

echo "Create ZAP Project Many BBLs table"
# Create the `zap_projects_many_bbls`` table
psql $BUILD_ENGINE -1 -f sca_aggregate/create_zap_projects.sql

# Aggregate KPDB projects to Elementary School Zones 
echo "Build Elementary School Zones Aggregate Table"
psql $BUILD_ENGINE -1 -f sca_aggregate/boundaries_es_zone.sql

echo "Build School Districts aggregate table"
# Aggregate KPDB projects to School District Zones
psql $BUILD_ENGINE -1 -f sca_aggregate/boundaries_school_districts.sql

echo "Build School Subdistricts aggregate tables"
# Aggregate KPDB projects to School Subdistrict Zones
psql $BUILD_ENGINE -1 -f sca_aggregate/boundaries_school_subdistricts.sql

psql $BUILD_ENGINE -c "ALTER TABLE _kpdb RENAME COLUMN geometry TO geom;"

echo "SCA Longform Aggregate tables are complete"
