# Checkbook NYC Exploratory Work and Join to CPDB

## Completed: 
- YAML file to upload Checkbook NYC input data to `edm-recipes` Digital Ocean 

## In progress: 
- YAML file to upload accepted CPDB geometries (2017-2022) and executive CPDB geometry (2023) to `edm-recipes` on Digital
- write empty(ish) bash/python scripts for `db-checkbook` product

## To do:
- write the code to pull down data from Digital Ocean
- Dea and Ali make their own branches and start working on smallest units of extractable code from our original notebooks, checking it into our projects, implementing, then submitting PR
- put data manipulation steps into sequential files, mimicing terminology of facdb (i.e. 01_dataloading.py, 02_build.py, etc)
- make a data product-level requirements doc if needed, and update top-level requirements doc (requirements.in) in monorepo with python modules that are generally applicable to the team's work

## Eventually to do: 
- (eventually) add recipe for updating Checkbook NYC data from website 
- upload final data to `edm-publishing` on S3 at the end
