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

## Todo

- [ ] upload relevant Checkbook NYC source data to Digital Ocean
- [ ] write the code to pull down data from Digital Ocean
- [ ] Dea and Ali make their own branches and start working on smallest units of extractable code from our original notebooks, checking it into our projects, implementing, then submitting PR
- [ ] put data manipulation steps into sequential files, mimicking terminology of facdb (i.e. 01_dataloading.py, 02_build.py, etc)
- [ ] make a data product-level requirements doc if needed, and update top-level requirements doc (requirements.in) in monorepo with python modules that are generally applicable to the team's work
- [ ] upload final data to `edm-publishing` on S3 at the end

## Eventually todo

- [ ] (eventually) add recipe for updating Checkbook NYC data from website
