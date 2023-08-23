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
