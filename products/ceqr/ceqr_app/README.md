# CEQR Application Data Pipelines

This repository contains workflows to generate the data behind Planning Lab's City Environmental Quality Review Application. Currently, Data Engineering generates the datasets for the following CEQR chapters:

+ 6- Community Facilities
+ 16- Transportation
+ 17- Air Quality

## General workflow

Each chapter consists of several datasets, organized into 'recipes.' In `/recipes`, you will find all of the pipelines currently maintained by Data Engineering. They are organized as follows:

```
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

### To build using github (NYCPlanning Members Only)

Running a recipe using github actions is easy! Simply open an
issue with a title containing `[build]` and the name of the recipe you'd like to run.
For example, `[build] ctpp_censustract_centroids`.

This will automatically trigger a build. Once complete, github will comment on your issue, provide a link with details about the run (this is where you can find runtime warnings and errors), then close out the issue.

### To build locally

1. at root directory (where the `ceqr` file is), run:

```
./ceqr run recipe {{ recipe name }}
```

e.g.

```
./ceqr run recipe nysdec_title_v_facility_permits
```

## Datasets

### 6. Community Facilities

+ ceqr_school_buildings
+ sca_capacity_projects
+ sca_e_projections_by_boro
+ sca_e_projections_by_sd
+ doe_significant_utilization_changes

### 16. Transportation

+ ctpp_censustract_centroids, ctpp_censustract_variables, ctpp_journey_to_work
+ nysdot_aadt
+ nysdot_functional_class
+ nysdot_traffic_counts
+ dot_traffic_cameras

### 17. Air Quality

+ atypical_roadways
+ dep_cats_permits
+ nysdec_air_monitoring_stations
+ nysdec_state_facility_permits
+ nysdec_title_v_facility_permits
+ tunnel_ventilation_towers
+ dcp_areas_of_concern
+ facilities_garages
