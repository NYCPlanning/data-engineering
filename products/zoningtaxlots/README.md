# NYC Zoning Tax Lot Database [![Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_build.yml/badge.svg)](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_build.yml)

The Zoning Tax Lot Database (ZTL) contains all tax lots from the specified version of the Department
of Finance’s Digital Tax Map. For each tax lot, it specifies the applicable zoning district(s),
commercial overlay(s), special purpose district(s), and other zoning related information.
**DCP assigns a zoning feature (includes zoning districts, special districts, and limited height**
**districts) to a tax lot if 10% or more of the tax lot is covered by the zoning feature.** For
commercial overlays, a tax lot is assigned a value if 10% or more of the tax lot is covered by the
commercial overlay and/or 50% or more of the commercial overlay feature is within the tax lot.
The zoning features are taken from the Department of City Planning NYC GIS Zoning Features.

## Important files

```bash
zoningtaxlots/
├── bash/
├── sql/ # scripts with transformation logic
├── README.md # this file
├── example.env 
├── recipe.yml # list of input datasets, AKA "recipes"
└── ztl.sh # entry point to run build steps
```

Additionally:

[`Github workflow`](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/zoningtaxlots_build.yml) - config file used to build ZTL via Github Actions

### Links

[ZTL on Bytes](https://www.nyc.gov/site/planning/data-maps/open-data.page#geocoding_application)

[ZTL on OpenData](https://data.cityofnewyork.us/City-Government/NYC-Zoning-Tax-Lot-Database/fdkv-4t4z/about_data)

For more in-depth information on ZTL and its schema, check out its [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-ZTL).
