source geolookup/config.sh

run_sql_command \
    "DROP TABLE IF EXISTS source_data_versions;
    CREATE TABLE source_data_versions (
        schema_name character varying,
        v character varying
    );"

# Import Data
import_recipe dpr_park_access_zone &
import_recipe fema_pfirms2015_100yr &
import_recipe fema_firms2007_100yr &
import_recipe dcp_censustracts 21c &
import_recipe dcp_censusblocks 21c
wait

echo "dataloading complete"

# Build
run_sql_file geolookup/2020/build.sql

# Export
mkdir -p factfinder/data/lookup_geo/2020 && (
    cd factfinder/data/lookup_geo/2020
    csv_export geolookup lookup_geo
)
