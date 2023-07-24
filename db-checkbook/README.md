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
    ./db-checkbook/checkbook.sh
    ```

    > 🚧 This is a work-in-progress and doesn't build the data product yet

## Completed: 
- YAML file to upload Checkbook NYC input data to `edm-recipes` Digital Ocean 
- write empty(ish) bash/python scripts for `db-checkbook` product

## In progress: 
- YAML file to upload accepted CPDB geometries (2017-2022) and executive CPDB geometry (2023) to `edm-recipes` on Digital
- [] add functions to `01_dataloading.py` for pulling down CPDB and Checkbook data from Digital Ocean

## Todo
- [ ] write code to pull down data from Digital Ocean
- [ ] add functions to clean Checkbook NYC data and collapse on capital projects
- [ ] add functions to merge CPDB geometries onto collapsed Checkbook NYC (creating Historical Spending dataset)
- [ ] add functions to categorize Checkbook NYC projects based on `Budget Code` and `Contract Purpose`
- [ ] add function to implement 'high sensitivity' Fixed Asset categorization to merged Historical Liquidations dataset 
- [ ] Dea and Ali make their own branches and start working on smallest units of extractable code from our original notebooks, checking it into our projects, implementing, then submitting PR
- [ ] put data manipulation steps into sequential files, mimicking terminology of facdb (i.e. 01_dataloading.py, 02_build.py, etc)
- [ ] make a data product-level requirements doc if needed, and update top-level requirements doc (requirements.in) in monorepo with python modules that are generally applicable to the team's work

## Eventually todo

- [ ] add recipe for updating Checkbook NYC data from website
- [ ] add recipe for automatically updating CPDB geometries
- [ ] upload final data to `edm-publishing` on S3 at the end
