# Capital Spending Database (CSDB) 

## Overview

## __About the Capital Spending Database (CSDB)__

The Capital Spending Database (CSDB), a data product produced by the New York City (NYC) Department of City Planning (DCP) Data Engineering team, presents the first-ever spatialized view of historical liquidations of the NYC capital budget. Each row in the dataset corresponds to a unique capital project and captures vital information such as the sum of all checks disbursed for the project, a list of agencies associated with the project, a category assignment based on keywords in project text fields (‘Fixed Asset’, ‘Lump Sum’, or ‘ITT, Vehicles, and Equipment’), and most importantly, geospatial information associated with the project when possible. 

...

## Data sources

Currently, the data product is created from two source datasets:

- NYC Office of the Comptroller Checkbook NYC
- DCP Capital Project DB

> Note: All source data is in our DigitalOcean S3 storage in the `edm-recipes` bucket

Checkbook NYC and the Capital Projects Database (CPDB). Checkbook NYC provides the information related to historical liquidations of the capital budget at the capital project level, while CPDB provides geospatial information for those projects. 

Checkbook NYC is an open-source dataset and tool from the NYC Comptroller’s Office that publishes every check disbursed by the city. For the Historical Liquidations dataset, we limited the scope of this data source to only those checks pertaining to capital spending, as defined by Checkbook NYC. 

CPDB is a data product produced by the NYC DCP Data Engineering team that “captures key data points on potential, planned, and ongoing capital projects sponsored or maintained by a capital agency in and around NYC” [source]. CPDB is updated three times per year in response to the Capital Commitment Plan (CCP). Because only capital projects reported in the current fiscal year (FY)’s CCP are reflected in CPDB, the Historical Liquidations dataset utilizes the previous adopted versions of CPDB, in addition to the most recent version of CPDB from the current FY.

### Limitations

Currently, roughly 25% of capital projects in Checkbook NYC can be assigned geometries. Because historical versions of CPDB only date back to 2017, the backwards scope of this data product is limited to geometries from 2017 onwards. Similarly, because Checkbook NYC only includes historical liquidations from 2010 onwards, the backwards scope of this data product is limited to capital projects from 2010 onwards. 

> 🚧 This data product is a work-in-progress. The outputs of this data product should not be cited as a definitive source of information until enhancements are complete.

## Build instructions (locally only)

1. Clone the repo and create `.env`

2. Open the repo in the defined devcontainer in VS code

3. Intall python packages

    ```bash
    ./bash/install_python_packages.sh python
    ```

    > Note: This is only necessary if devcontainer does not have necessary packages installed already. This can be checked first via `pip list`.

4. Run the following command(s) from a terminal

    ```bash
    cd projects/db-checkbook
    python3 -m build_scripts.dataloading
    python3 -m build_scripts.build
    python3 -m build_scripts.export
    ```

    > 🚧 This is a work-in-progress and doesn't build the data product yet

## Completed: 
- YAML file to upload Checkbook NYC input data to `edm-recipes` Digital Ocean 
- write empty(ish) bash/python scripts for `db-checkbook` product
- YAML file to upload accepted CPDB geometries (2017-2022) and executive CPDB geometry (2023) to `edm-recipes` on Digital
- `build_scripts/dataloading.py` correctly pulls down CPDB and Checkbook input data from DO
- `build_scripts/build.py` cleans, transforms and merges the input data sources
- `build_scripts/export.py` exports the final dataset to DO
- `build_scripts/summarization.py` provides relevant summary statistics about the output dataset
- `test/test_build_output.py` is a unit test suite that uses pytest to validate outputs of build


## In progress: 

- exploring improving geospatial information by layering in parks properties dataset
- working on adding a page for db-checkbook to the QAQC streamlit app


## Todo
- [ ] make a data product-level requirements doc if needed, and update top-level requirements doc (requirements.in) in monorepo with python modules that are generally applicable to the team's work

## Eventually todo
- [ ] add recipe for updating Checkbook NYC data from website
- [ ] add recipe for automatically updating CPDB geometries