# Geospatial Lookup

This directory has the code for generating geospatial look up (`geolookup` or `lookup_geo`)

## Instructions
1. Make sure your `.env` file contains a `BUILD_ENGINE`
2. run command: `python3 -m geolookup -g 2020` to generate geolookup for geography year `2020`

> Note for geography `2010_to_2020` it's using the same `lookup_geo.csv` as geography `2020`, there's additionally a ratio.csv that would convert 2010 tracts to 2020 tracts
