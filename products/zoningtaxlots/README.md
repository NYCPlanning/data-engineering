# NYC Zoning Tax Lot Database [![Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_build.yml/badge.svg)](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_build.yml)

The Zoning Tax Lot Database includes the zoning designations and zoning map associated with a specific tax lot.  Using the tax lots in the Department of Finance Digital Tax Map, zoning features from the Department of City Planning NYC GIS Zoning Features, and spatial analysis tools DCP assigns
a zoning district (includes commercial overlays, special districts, and limited height districts) to a tax lot if 10% or more of the tax lot is covered by the zoning feature and/or 50% or more of the zoning feature is within a tax lot.

Up to four zoning districts are reported for each tax lot intersected by zoning boundary lines, and the order in which zoning districts are assigned is based on how much of the tax lot is covered by each zoning district

For example: If tax lot 98 is divided by zoning boundary lines into four sections - Part A, Part B, Part C and Part D. Part A represents the largest portion of the lot, Part B is the second largest portion of the lot, Part C represents the third largest portion of the lot and Part D covers the smallest portion of the tax then ZONING DISTRICT 4 will contain the zoning associated with Part D.

The final data table is provided in a comma–separated values (CSV) file format where each record reports information on a tax lot and BBL is the unique ID.

Instructions on how to build the Zoning Tax Lot Database are included in the zoningtaxlots_build folder.

## Output Files: 
+ [qc_bbldiff.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-zoningtaxlots/latest/output/qc_bbldiffs/qc_bbldiffs.zip)
+ [source_data_versions.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-zoningtaxlots/latest/output/source_data_versions.csv)
+ [zoningtaxlots_db.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-zoningtaxlots/latest/output/zoningtaxlot_db.csv)

## QAQC
QAQC metrics comparing versions of ZTL can be found on the [Data Engineering QAQC Portal](https://edm-data-engineering.nycplanningdigital.com/?page=Zoning+Tax+Lots).

## Instructions 
### Building via GHA
> 🚧 This section is a work in progress as we move our existing data products into the Data Engineering monorepo.

### Build instructions
1. Clone the repo and create `.env`
2. Open the repo in the defined devcontainer in VS Code
3. Run the following commands at the root directory of the Zoning Tax Lot Database product (`/zoningtaxlots`):
```bash
./ztl.sh dataloading
./ztl.sh build
./ztl.sh qaqc
```
> 🚧 The dataloading step is currently failing in the monorepo.

# Data update details

- Make sure zoning shapefiles, DTM are up-to-date (check [recipe](https://github.com/NYCPlanning/recipes))
- You can [create an issue](https://github.com/NYCPlanning/recipes/issues/new?assignees=aferrar%2C+croswell81&labels=dataloading&template=zoning-tax-lots-bulk-load.md&title=%5Bztl%5D+Zoning+Tax+Lots+Bulk+Load) to import source data
  - Before opening this issue, please confirm that the source data is uploaded to `edm-publishing/datasets/<name>/staging` by GIS.
- You will see source data versions in the issue comment when the action finishes running
