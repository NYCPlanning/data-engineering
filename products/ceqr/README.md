# CEQR Application Data Pipelines

This repository contains workflows to generate the data behind Planning Lab's City Environmental Quality Review Application. Currently, Data Engineering generates the datasets for the following CEQR chapters:

- 6- Community Facilities
- 16- Transportation
- 17- Air Quality

## General workflow

Each chapter consists of several datasets, organized into 'recipes.' In `/recipes`, you will find all of the pipelines currently maintained by Data Engineering. They are organized as follows:

```bash
recipes/
├── <dataset name>
│   ├── build.sql (or build.py, or both)
│   ├── create.sql
│   ├── output
│   │   ├── < csv output >.csv
│   │   ├── < shp output >.zip
│   │   └── version.txt
│   └── runner.sh
```

In general, the `build` scripts retrieve relevant columns from the input data, either by pulling from an open data source or by quering Data Engineering's RECIPE database. If needed, `build.py` also geocodes the data.

This then gets passed to the EDM production database using `create.sql`, where final filtering, mapping, and geometry creation happens. Outputs also get pushed to a staging folder in DigitalOcean Spaces. Publishing from staging requires an additional call to the CLI.

## Build instructions

At the momement, these datasets are built locally rather than using github actions. Since geosupport is used during builds, running builds via the DE devcontainer is recommended.

Archiving source data should also be done locally. This is because, for these builds, source data is expected to be in the `recipe` database in the `edm-data` database cluster. Our relevant github action doesn't expose the `output-format` option for the library CLI so the option `postgres` cannot be chosen.

For output file upload to S3 to work from the devcontianer, it may be necssary to run the following command to setup the `mc` client:

```bash
mc config host add spaces "$AWS_S3_ENDPOINT" "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" --api S3v4
```

### Data flow diagram

[data flow diagram](./docs/diagrams/dataflow_ceqr.drawio.png)

1. Archive new source data to the `recipe` database in the `edm-data` cluster.

   ```bash
   library archive --name {{ source dataset name }}  --version {{ source dataset version }} --output_format postgres --postgres_url $RECIPE_ENGINE

   # for example
   library archive --name sca_capacity_projects_current --version 20241226 --output-format postgres --postgres-url $RECIPE_ENGINE
   ```

2. Confirm a successful archival by checking for a table with the name of the source data version in the dataset's schema (e.g. `sca_capacity_projects_current.20241226`).

3. Navigate to the CEQR product directory via `cd products/ceqr/`.

4. Run a dataset's build.

   ```bash
   ./ceqr run recipe {{ ceqr dataset name }}

   # for example
   ./ceqr run recipe nysdec_title_v_facility_permits
   ```

5. Confirm a succesful build by checcking for files in S3

## Datasets

### 6. Community Facilities

- ceqr_school_buildings
- sca_capacity_projects
- sca_e_projections_by_boro
- sca_e_projections_by_sd
- doe_significant_utilization_changes

### 16. Transportation

- ctpp_censustract_centroids, ctpp_censustract_variables, ctpp_journey_to_work
- nysdot_aadt
- nysdot_functional_class
- nysdot_traffic_counts
- dot_traffic_cameras

### 17. Air Quality

- atypical_roadways
- dep_cats_permits
- nysdec_air_monitoring_stations
- nysdec_state_facility_permits
- nysdec_title_v_facility_permits
- tunnel_ventilation_towers
- dcp_areas_of_concern
- facilities_garages
