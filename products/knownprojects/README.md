# Known Projects Database (KPDB)

This repo contains code for creating the Known Projects Database (KPDB). The build process has multiple automated phases, separated by manual review. 

For detailed information on the tables created throughout the build process, see the [build environment table descriptions](https://github.com/NYCPlanning/db-knownprojects/wiki/Build-environment-tables).

## Instructions

### Build

- Navigate to the bash scripts: `cd knownprojects_build`
- Load data: `./01_dataloading.sh`
- Build database: `./02_build.sh`
- Export: `./03_export.sh`
