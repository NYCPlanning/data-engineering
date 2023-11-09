#!/bin/bash
source ../../bash/utils.sh
set_error_traps

echo "Preprocess for all aggregations"
run_sql_command "ALTER TABLE _kpdb RENAME COLUMN geom TO geometry;"

echo "Create ZAP Project Many BBLs table"
run_sql_file aggregate/create_zap_projects.sql

## Do SCA aggregation
echo "Create the longfrom SCA Aggregate Tables..."

# Preprocess the tables to standardize geometry column name 
echo "Preprocess column names to standardize"
run_sql_file aggregate/sca/preprocessing.sql

# HACK disable to optimize
# Aggregate KPDB projects to Elementary School Zones 
# echo "Build Elementary School Zones Aggregate Table"
# run_sql_file aggregate/sca/boundaries_es_zone.sql

# # Aggregate KPDB projects to School District Zones
# echo "Build School Districts aggregate table"
# run_sql_file aggregate/sca/boundaries_school_districts.sql

# HACK optimize this query
# Aggregate KPDB projects to School Subdistrict Zones
echo "Build School Subdistricts aggregate tables"
run_sql_file aggregate/sca/boundaries_school_subdistricts.sql

echo "SCA sggregations are complete"

# HACK disable to optimize
# ## Do other aggregation
# echo "Preprocess column names to standardize"
# run_sql_file aggregate/preprocessing.sql

# # Aggregate KPDB projects to Community Districts
# echo "Build Community Districts aggregate tables"
# run_sql_file aggregate/census_tracts.sql
# run_sql_file aggregate/ntas.sql
# run_sql_file aggregate/cdtas.sql
# run_sql_file aggregate/community_districts.sql

# echo "Other aggregations are complete"

# run_sql_command "ALTER TABLE _kpdb RENAME COLUMN geometry TO geom;"

# echo "All aggregations are complete"
