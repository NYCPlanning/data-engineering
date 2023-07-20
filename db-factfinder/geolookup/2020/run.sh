source geolookup/config.sh

# Import Data
import_public dpr_park_access_zone &
import_public fema_pfirms2015_100yr &
import_public fema_firms2007_100yr &
import_public dcp_censustracts 21c &
import_public dcp_censusblocks 21c
wait

echo "dataloading complete"

# Build
psql $BUILD_ENGINE -f geolookup/2020/build.sql

# Export
mkdir -p factfinder/data/lookup_geo/2020 && (
    cd factfinder/data/lookup_geo/2020
    CSV_export geolookup lookup_geo
)
