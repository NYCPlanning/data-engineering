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

Start in the root of the repo. Make sure your .env file is reflected in your bash shell, then run our env setup. You will need variables defined in your .env file

```
BUILD_ENGINE_HOST
BUILD_ENGINE_USER
BUILD_ENGINE_PASSWORD
BUILD_ENGINE_PORT
BUILD_ENGINE_DB
BUILD_ENGINE_SCHEMA
```

```bash
source ./bash/utils.sh
set_env .env
```

Some finagling is needed to get some of the env vars properly. For now, this is manual

```bash
# replace dashes with underscores to create a valid postgres schema name
export BUILD_ENGINE_SCHEMA=$(echo ${BUILD_NAME} | tr - _ | tr '[:upper:]' '[:lower:]')
# align with tests schema name created by dbt
export BUILD_ENGINE_SCHEMA_TESTS=$(echo ${BUILD_ENGINE_SCHEMA}__tests)
# set postgres schema search path to prioritize BUILD_ENGINE_SCHEMA
export BUILD_ENGINE=${BUILD_ENGINE_SERVER}/${BUILD_ENGINE_DB}?options=--search_path%3D${BUILD_ENGINE_SCHEMA},public
```

Then, cd into this folder. Setup dbt.

```bash
dbt deps
dbt debug
```

### Plan/Load
Compile the `recipe` (into `recipe.lock.yml`)

```bash
python3 -m dcpy.lifecycle.builds.plan
```

Then, load source data into the db

```bash
python -m dcpy.lifecycle.builds.load load
```

Load the "seed" tables (small tables checked into the repo, in `./seeds`) into the db

```bash
dbt build --select config.materialized:seed --indirect-selection=cautious --full-refresh
```

Test source tables (the data that we've loaded into the db prior to running any transformations)

```bash
dbt test --select "source:*"
```

### Build
The actual transformations! You don't have to do this in chunks. Each stage - staging, intermediate, product - corresponds to a folder in `./models` which contain sql files for the various sql models in this pipeline

```bash
dbt build --select staging
dbt build --select intermediate
dbt build --select product
```

This also runs tests at each stage of the pipeline

### Export

Make sure our bash utils are sourced.

```bash
source ../../bash/utils.sh
```

Then run the export script

```bash
./bash/export.sh
```

### Upload

```bash
dcpy lifecycle builds artifacts builds upload --product db-green-fast-track --acl public-read
```
