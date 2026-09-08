source geolookup/config.sh

# Build
run_sql_file geolookup/2020/build.sql

# Export
mkdir -p factfinder/data/lookup_geo/2020 && (
    cd factfinder/data/lookup_geo/2020
    csv_export geolookup lookup_geo
)
