# db-developments

Processing DOB Job Application and Certificate of Occupancy data to identify jobs that will increase or decrease the number of units

## Instructions

1. Create `.env` file and set environmental variables: `RECIPE_ENGINE`, `BUILD_ENGINE`, `EDM_DATA`

2. Start postgis docker container for local database:
```bash
docker run --name <custom_container_name> -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgis/postgis
```

3. Check connection (e.g. via database app like Postico)
> 💡 Note: If failing to connect on mac, try `brew services stop postgresql`

4. Run scripts:
```bash
./bash/01_dataloading.sh
...
```

## Development File Download

> Note that these files are not official releases, they are provided for QAQC purposes only, for official releases, please checkout [Bytes of the Big Apple](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-housing-database.page#housingdevelopmentproject)

#### Main Tables

  | Devdb | HousingDB
-- | -- | --
CSV | [devdb.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/devdb.csv) | [housing.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/housing.csv)
Shapefile | [devdb.shp.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/devdb.shp.zip) | [housing.shp.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/housing.shp.zip)

#### Aggregation Tables 2020 Geographies

[block](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_block_2020.csv) |
[tract](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_tract_2020.csv) |
[NTA](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_nta_2020.csv) |
[CDTA](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_cdta_2020.csv)

#### Aggregation Tables 2010 Geographies

[block](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_block_2010.csv) |
[tract](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_tract_2010.csv) |
[commntydst](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_commntydst_2010.csv) |
[councildst](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_councildst_2010.csv) |
[NTA](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/aggregate_nta_2010.csv)

#### All files [bundle.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-developments/main/latest/output/output.zip)

## Published Versions

<details><summary>20Q4</summary>
  
    | HousingDB | Devdb
 -- | -- | --
CSV        | [dcp_housing.csv](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_housing/20Q4/dcp_housing.csv) | [dcp_developments.csv](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_developments/20Q4/dcp_developments.csv)
Zipped CSV | [dcp_housing.csv](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_housing/20Q4/dcp_housing.csv.zip)  |  [dcp_developments.csv.zip](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_developments/20Q4/dcp_developments.csv.zip)
Shapefile  |  [dcp_housing.shp.zip](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_housing/20Q4/dcp_housing.shp.zip) | [dcp_developments.shp.zip](https://nyc3.digitaloceanspaces.com/edm-recipes/datasets/dcp_developments/20Q4/dcp_developments.shp.zip)
  
</details>

# Data update details

## Update code

- [ ] Add most recent full/half year to aggregate tables

## Update source data

- [ ] Update version.env to:

```bash
# DevDB versions for latest release
VERSION=22Q4 #Current Build Version of DevDB
VERSION_PREV=22Q2 #Previous Build Version of DevDB

# DevDB data details
CAPTURE_DATE= #Reference date for the DOB records (i.e. 22Q4 is a snapshot of all DOB records filed before 01-01-2023)
CAPTURE_DATE_PREV= #Reference date for the DOB records (i.e. 22Q4 is a snapshot of all DOB records filed after 2022-07-01)

# external tools versions
GEOSUPPORT_DOCKER_IMAGE_VERSION=22.3.0

# input spatial boundaries versions
GEOSUPPORT_VERSION=22c
DCP_MAPPLUTO_VERSION=
DOITT_ZIPCODE_VERSION=
DOF_VERSION=

# input data versions
COUNCIL_MEMBERS_VERSION=
DOE_ZONES_VERSION=
DOE_SUBDISTRICTS_VERSION=
HNY_VERSION=
DOB_DATA_DATE=
DOB_NOW_APPS_VERSION=
DOB_NOW_PERMITS_VERSION=
DOB_COFOS_VERSION=
DOITT_BUILDINGS_VERSION=
DOITT_BUILDINGS_HISTORICAL_VERSION=
```

### Make sure the following are up-to-date in Digital Ocean

#### City Administrative Boundaries
- [ ] `dcp_cdboundaries`  
- [ ] `dcp_cb2010`  
- [ ] `dcp_ct2010` 
- [ ] `dcp_cb2020` 
- [ ] `dcp_ct2020` 
- [ ] `dcp_school_districts` 
- [ ] `dcp_boroboundaries_wi`
- [ ] `dcp_councildistricts` 
- [ ] `dcp_firecompanies` 
- [ ] `dcp_policeprecincts` 
- [ ] `doitt_zipcodeboundaries` 
- [ ] `dof_shoreline`
#### General

- [ ] `dcp_mappluto_wi`
- [ ] `dof_shoreline` updated with zoningtaxlots, safe to ignore
- [ ] `council_members` [check opendate](https://data.cityofnewyork.us/City-Government/Council-Members/uvw5-9znb)
- [ ] `doitt_buildingfootprints` [check opendata](https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh)
- [ ] `doitt_buildingfootprints_historical` [check opendata](https://data.cityofnewyork.us/Housing-Development/Building-Footprints-Historical-Shape/s5zg-yzea)
- [ ] `doitt_zipcodeboundaries` -> never changed, safe to ignore
- [ ] `doe_school_subdistricts` -> received from capital planning
- [ ] `doe_eszones` -> the url for this changes year by year, [search on opendata](https://data.cityofnewyork.us/browse?q=school+zones)
- [ ] `doe_mszones` -> same as above
- [ ] `hpd_hny_units_by_building` [check opendata](https://data.cityofnewyork.us/Housing-Development/Housing-New-York-Units-by-Building/hg8x-zxpr) and [run Data Sync action](https://github.com/NYCPlanning/db-developments/actions/workflows/data_sync.yml)
- `hny_geocode_results` 

#### DOB data

- [ ]  `dob_cofos` -> manually updated, received by email
- [ ]  `dob_jobapplications` [check actions](https://github.com/NYCPlanning/recipes/actions?query=workflow%3A%22DOB+pull+for+HED%22)
- [ ]  `dob_permitissuance` [check actions](https://github.com/NYCPlanning/recipes/actions?query=workflow%3A%22DOB+pull+for+HED%22)
- [ ] `dob_now_applications` -> DOB contacts us via email that the data is ready, the data is downloaded from the DOB FTP using credentials, manually uploaded to DO and ingested via Data Library pipeline
- [ ] `dob_now_permits` -> DOB contacts us via email that the data is ready, the data is downloaded from the DOB FTP using credentials, manually uploaded to DO and ingested via Data Library pipeline
