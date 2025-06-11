# Green Fast Track (GFT)

Green Fast Track (GFT) determines, for all lots in New York City, if a new construction project to build housing is eligible for streamlined environmental review.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/green_fast_track/recipe.yml)

## Links

[GFT Overview](https://www.nyc.gov/site/planning/plans/green-fast-track/green-fast-track-overview.page)
[GFT on Arcgis Online](https://dcp.maps.arcgis.com/home/item.html?id=16e45892aeef4ff0978172a04b54fc58)
[Data Dictionary](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-capital-planning-database.page)

For more in-depth information, check out the data product's [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-GFT)

## Running locally

### Setup
Make sure you have a BUILD_NAME env var. If not, set it

`export BUILD_NAME=my-build-name`

From the root of the repo, run

`./bash/build_env_setup.sh`

Then, cd into this folder. Setup dbt.

`dbt deps`
`dbt debug`

### Plan/Load
Compile the `recipe` (into `recipe.lock.yml`)

`python3 -m dcpy.lifecycle.builds.plan`

Then, load source data into the db

`python -m dcpy.lifecycle.builds.load load`

Load the "seed" tables (small tables checked into the repo, in `./seeds`) into the db

`dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh`

Test source tables (the data that we've loaded into the db prior to running any transformations)

`dbt test --select "source:*"`

### Build
The actual transformations! You don't have to do this in chunks. Each stage - staging, intermediate, product - corresponds to a folder in `./models` which contain sql files for the various sql models in this pipeline

`dbt build --select staging`
`dbt build --select intermediate`
`dbt build --select product`

This also runs tests at each stage of the pipeline

### Export

Make sure our bash utils are sourced.

`source ../../bash/utils.sh`

Then run the export script

`./bash/export.sh`

### Upload

`python3 -m dcpy.connectors.edm.publishing upload --product db-green-fast-track --acl public-read`
