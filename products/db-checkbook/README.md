# Checkbook NYC Exploratory Work and Join to CPDB

## Overview

...

## Data sources

- NYC Office of the Comptroller Checkbook NYC
- DCP Capital Project DB

> Note: All source data is in our DigitalOcean S3 storage in the `edm-recipes` bucket

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
- `01_dataloading.py` correctly pulls down CPDB and Checkbook input data from DO
- `02_build.py` cleans, transforms and merges the input data sources
- `03_export.py` exports the final dataset to DO
- uploaded final output dataset to `edm-publishing` on DO


## In progress: 
- writing unit tests using pytest to validate outputs of build


## Todo
- [ ] make a data product-level requirements doc if needed, and update top-level requirements doc (requirements.in) in monorepo with python modules that are generally applicable to the team's work

## Eventually todo
- [ ] add recipe for updating Checkbook NYC data from website
- [ ] add recipe for automatically updating CPDB geometries