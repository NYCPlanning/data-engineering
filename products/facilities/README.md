# db-facilities

## Outputs:
| File                                                                                                                               | Description                                                                                                           |
| ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| [facilities.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/facilities.csv)               | CSV version of facDB, as of the latest build on the `main` branch                                                     |
| [facilities.gdb.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/facilities.gdb.zip)       | GeoDatabase version of facDB, as of the latest build on the `main` branch                                             |
| [facilities.shp.zip](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/facilities.shp.zip)       | Shapefile version of facDB, as of the latest build on the `main` branch                                               |
| [qc_captype.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_captype.csv)               | QAQC for consistency in capacity type                                                                                 |
| [qc_classification.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_classification.csv) | QAQC for consistency in grouping information                                                                          |
| [qc_diff.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_diff.csv)                     | QAQC for change in distribution of number of records by facsubgroup/group/domain between current and previous version |
| [qc_mapped.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_mapped.csv)                 | QAQC for change in mapped records by facdomain, facgroup, facsubgrp, factype, and datasource                          |
| [qc_operator.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_operator.csv)             | QAQC for consistency in operator information                                                                          |
| [qc_oversight.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_oversight.csv)           | QAQC for consistency in oversight information                                                                         |
| [qc_recordcounts.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_recordcounts.csv)     | QAQC for number of records in source vs facdb                                                                         |
| [qc_subgrpbins.csv](https://edm-publishing.nyc3.digitaloceanspaces.com/db-facilities/main/latest/output/qc_subgrpbins.csv)         | QAQC for number of BINs per subgroup                                                                                  |

## QAQC:

[Streamlit QAQC Page](https://de-qaqc.nycplanningdigital.com/?page=Facilities+DB)

## Development:

### Configurations

- We strongly advise that you use VScode for development of this project
- The `.devcontainer` contains all development configuration you would need, including:
  - `psql` is preinstalled
  - `python-geosupport` is installed with 21a geosupport
  - `poetry` is installed and initialized
  - `pre-commit` is also preconfigured and no additional setup is needed
- If you do not have vscode setup, don't worry, you can still develop using docker-compose
  - initialize docker-compose: `./facdb.sh init`
  - the facdb.sh script is a one to one wrapper of the python facdb cli, e.g. `./facdb.sh --help`
  - docker-compose is also used to test PR and build the dataset in github actions

### Pipeline development with the `facdb` cli

> 💡 Note: the cli documentation might not always be up-to-date, to see the latest commands and features, use `facdb --help`

- Create a pipeline function under the name of the function, e.g. `dcp_colp`
- If you don't want to run every command with `poetry run`, we recommend you activate the virtual environment by `poetry shell`
- `facdb --help` to show instructions

```console
foo@bar:~$ facdb --help
Usage: facdb [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  build        Building facdb based on facdb_base
  clear        clear will clear the cached dataset created while reading a...
  dataloading  Load SQL dump datasets from data library e.g.
  export       create file export
  init         Initialize empty facdb_base table and create procedures and...
  qaqc         Running QAQC commands
  run          This function is used to execute the python portion of a...
  sql          this command will execute any given sql script against the...
```

<details><summary>Examples</summary>

- `facdb init` initialization of the database with `facdb_base`, functions and stored procedures
- `facdb dataloading` load supplementry datasets from data-library (e.g. `dcp_mappluto_wi`, `doitt_buildingcentroids`)
- `facdb run`
  - `facdb run -n nysed_activeinstitutions` to execute both the python and sql part specified in `datasets.yml`
  - `facdb run -n nysed_activeinstitutions --python` to execute the python part only
  - `facdb run -n nysed_activeinstitutions --sql` to execute the sql part only
  - `facdb run -n nysed_activeinstitutions --python --sql` is the same as `facdb run -n nysed_activeinstitutions`
  - `facdb run --all` to run all available data piplines defined in `datasets.yml`
- `facdb sql`
  - `facdb sql -f facdb/sql/dcp_colp.sql` to execute one script
  - `facdb sql -f facdb/sql/dcp_colp.sql -f some/other/script.sql` to execute multiple scripts
- `facdb clear`
  - `facdb clear -n nysed_activeinstitutions` to clear cache for nysed_activeinstitutions
  - `facdb clear --all` to clear all cache for all datasets
- `facdb build` build facdb from loaded source tables and produce QAQC tables

</details>

# FacDB Source Data Updates

Like most of our data products, source data must be updated in data library before FacDB is run. As there are are many source datasets with varied update processes, this issue template should be opened to track progress towards updating all source data

**All source data listed is to be uploaded as .csv files**

## Geosupport

- [ ] Ensure latest Geosupport version is present in production EDM Docker images

## Version Env

- [ ] Update previous version for QCQA in `version.env` file

## Scraped by data library

- [ ] bpl_libraries
  Source: Scraped from BPL website
  Source url: https://www.bklynlibrary.org/locations/json
- [ ] nypl_libraries
  Source: Scrape from NYPL website
  Source url: https://www.nypl.org/locations/list
- [ ] uscourts_courts
  Source: Court locator for NY state
  Source url: http://www.uscourts.gov/court-locator/city/New%20York/state/NY
- [ ] dcp_colp 
  Source: Bytes
  Source url: https://www1.nyc.gov/site/planning/data-maps/open-data.page#city_facilities
  Need to specify version when archiving the data (check on the website for date last updated).
- [ ] nysoasas_programs
  Source: Scraped from OASAS website
  Source url: https://webapps.oasas.ny.gov/providerDirectory/index.cfm?search_type=2
  Need to set version to today’s date as DD-Mon-YY. Example: 13-Nov-20

## Source data from OpenData

To see if a dataset needs to be uploaded, check date last updated in open data/bytes against version in data library.

- [ ] dca_operatingbusinesses https://data.cityofnewyork.us/Business/Legally-Operating-Businesses/w7w3-xahh
- [ ] dcp_facilities_with_unmapped https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-selfac.page
- [ ] dcla_culturalinstitutions https://data.cityofnewyork.us/Recreation/DCLA-Cultural-Organizations/u35m-9t32
- [ ] dfta_contracts https://data.cityofnewyork.us/Social-Services/Department-for-the-Aging-NYC-Aging-All-Contracted-/cqc8-am9x
  NOTE: `providertype` column has two types of meal categories. One of them, **CITY MEALS ADMINISTRATIVE SERVICES CONTRACTS**, should only have one record (city meals). If there are more records in this category, need to revise code in `dfta_contracts.sql`.
- [ ] doe_busroutesgarages https://data.cityofnewyork.us/Transportation/Routes/8yac-vygm

- [ ] dot_pedplazas https://data.cityofnewyork.us/Transportation/Routes/k5k6-6jex

- [ ] sca_enrollment_capacity https://data.cityofnewyork.us/Education/Enrollment-Capacity-And-Utilization-Reports-Target/8b9a-pywy
- [ ] dohmh_daycare https://data.cityofnewyork.us/Health/DOHMH-Childcare-Center-Inspections/dsg6-ifza
- [ ] dpr_parksproperties https://nycopendata.socrata.com/Recreation/Parks-Properties/enfh-gkve
  NOTE: DPR open data table URLs are not consistent. Be sure to double-check before running from the recipes app.
- [ ] dsny_garages https://data.cityofnewyork.us/Environment/DSNY-Garages/xw3j-2yxf
- [ ] dsny_specialwastedrop https://data.cityofnewyork.us/Environment/DSNY-Special-Waste-Drop-off-Sites/242c-ru4i
- [ ] dsny_donatenycdirectory https://data.cityofnewyork.us/Environment/DSNY-DonateNYC-Directory/gkgs-za6m
- [ ] dsny_leafdrop https://data.cityofnewyork.us/Environment/Leaf-Drop-Off-Locations-in-NYC/8i9k-4gi5
- [ ] dsny_fooddrop https://data.cityofnewyork.us/Environment/Food-Scrap-Drop-Off-Locations-in-NYC/if26-z6xq
- [ ] dsny_electronicsdrop https://data.cityofnewyork.us/Environment/Electronics-Drop-Off-Locations-in-NYC/wshr-5vic
- [ ] dycd_afterschoolprograms https://data.cityofnewyork.us/Education/DYCD-after-school-programs/mbd7-jfnc
- [ ] fdny_firehouses https://data.cityofnewyork.us/Public-Safety/FDNY-Firehouse-Listing/hc8x-tcnd
- [ ] hhc_hospitals https://data.cityofnewyork.us/Health/Health-and-Hospitals-Corporation-HHC-Facilities/f7b6-v6v3
- [ ] hra_jobcenters https://data.cityofnewyork.us/Business/Directory-Of-Job-Centers/9d9t-bmk7
- [ ] hra_medicaid https://data.cityofnewyork.us/City-Government/Medicaid-Offices/ibs4-k445
- [ ] hra_snapcenters https://data.cityofnewyork.us/Social-Services/Directory-of-SNAP-Centers/tc6u-8rnp
- [ ] moeo_socialservicesitelocations https://data.cityofnewyork.us/City-Government/Verified-Locations-for-NYC-City-Funded-Social-Serv/2bvn-ky2h
- [ ] nycha_communitycenters https://data.cityofnewyork.us/Social-Services/Directory-of-NYCHA-Community-Facilities/crns-fw6u
- [ ] nycha_policeservice https://data.cityofnewyork.us/Housing-Development/NYCHA-PSA-Police-Service-Areas-/72wx-vdjr
  NOTE: this data is shown as a map on the website.
- [ ] nysdec_solidwaste https://data.ny.gov/Energy-Environment/Solid-Waste-Management-Facilities/2fni-raj8
- [ ] nysdoh_healthfacilities https://health.data.ny.gov/Health/Health-Facility-General-Information/vn5v-hh5r
- [ ] nysdoh_nursinghomes https://health.data.ny.gov/Health/Nursing-Home-Weekly-Bed-Census-Last-Submission/izta-vnpq
- [ ] nysed_nonpublicenrollment http://www.p12.nysed.gov/irs/statistics/nonpublic/
- [ ] nysomh_mentalhealth https://data.ny.gov/Human-Services/Local-Mental-Health-Programs/6nvr-tbv8
- [ ] nysopwdd_providers https://data.ny.gov/Human-Services/Directory-of-Developmental-Disabilities-Service-Pr/ieqx-cqyk
- [ ] nysparks_historicplaces https://data.ny.gov/Recreation/National-Register-of-Historic-Places/iisn-hnyv
- [ ] nysparks_parks https://data.ny.gov/Recreation/State-Park-Facility-Points/9uuk-x7vh
- [ ] qpl_libraries https://data.cityofnewyork.us/Education/Queens-Library-Branches/kh3d-xhq7
- [ ] sbs_workforce1 https://data.cityofnewyork.us/dataset/Center-Service-Locations/6smc-7mk6
- [ ] usdot_airports https://geodata.bts.gov/datasets/aviation-facilities/explore?location=50.755910%2C-117.686932%2C22.58&showTable=true
  Use data-library to extract and archive data. Note, data-library uses a different link to extract the data. This link is provided as a reference for last date updated.  
- [ ] usdot_ports https://data-usdot.opendata.arcgis.com/datasets/usdot::docks/about
  Use data-library to extract and archive data

## Manually check data for updates

The source datasets don't currently provide an API or export option on their websites. The data engineering team initially created the datasets 
and continues to maintain them internally, relying on the information available on the websites. Also, the source data websites don't report 
date updated as neatly as the open datasets, have to look at data itself. 

- [ ] fbop_corrections https://www.bop.gov/locations/list.jsp
  When searching by state, there should be 5 NY prisons, 3 of which are in NYC (Brooklyn/New York)
- [ ] nycdoc_corrections  https://www1.nyc.gov/site/doc/about/facilities-locations.page
  Source: NYCDOC locations directory
- [ ] nycourts_courts http://www.nycourts.gov/courts/nyc/criminal/generalinfo.shtml#BRONX_COUNTY
- [ ] nysdoccs_corrections https://doccs.ny.gov/find-facility
  Hand check for 1 facility in queens, 1 facility in Manhattan, 0 in the other 3 boros. Only look at the correctional facility locations, not the offices.

## Manual download

Manually download the following datasets to your local machine and make tweaks if needed per individual instructions. After downloading, 
use data library CLI to archive the data to S3. Refer to the dataset templates in data library to see where it expects the data – that's where 
it will search when archiving. 
Sample CLI command ran locally to archive the `foodbankny_foodbanks` dataset to S3:
```bash 
library archive --s3 --name foodbankny_foodbanks --latest --version 20240108 --output-format csv
```


- [ ] nysdec_lands https://data.gis.ny.gov/datasets/84b4cce8a8974c31a1c5584540f3aaae_0/about
- [ ] doe_lcgms https://data.cityofnewyork.us/Education/LCGMS-DOE-School-Information-Report/3bkj-34v2
  This dataset is updated for CEQR
- [ ] foodbankny_foodbanks http://www.foodbanknyc.org/get-help/
  1. head to http://www.foodbanknyc.org/get-help/
  2. navigate to the map and make a copy of the map. The map can be found in the "Find Food Near You" drop-down menu. Note, you need to be logged in with a google account in order to have an option to copy the map.
  3. After making a copy, click on the three dots next to the target layer and click "Export Data" and export as a csv
  4. Rename the file (still as a csv) to match Food_Bank_For_NYC_Open_Members_as_of_DATE(YYYYMMDD). You will need to convert the existing date format MMDDYY to YYYYMMDD so that the version matches existing date format standard in data library. Example: Food_Bank_For_NYC_Open_Members_as_of_20240108.csv
  5. place it at the library/tmp folder
  6. then run library archive --name foodbankny_foodbanks with the -version flag set to the DATE in the file path
  url: "http://www.foodbanknyc.org/get-help/"
  dependents: []

- [ ] nysed_activeinstitutions  https://eservices.nysed.gov/sedreports/list?id=1
  Active Institutions with GIS coordinates and OITS Accuracy Code - Select by County__ CSV. Note that .csv data is automatically downloaded without comma delimiter. Exporting to csv from numbers is one way to get around this issue. (Exporting as an xls and converting to a csv is also an option)
- [ ] usnps_parks https://irma.nps.gov/DataStore/Reference/Profile/2302064
  NOTE: the final number in the URL (2302064) is not always stable. If the data is missing, search through the home.


### Will receive via email or FTP

- [ ] dot_bridgehouses
- [ ] dot_ferryterminals
- [ ] dot_mannedfacilities
- [ ] dot_publicparking
- [ ] doe_universalprek (https://maps.nyc.gov/prek/data/pka/pka.csv)

## Last step

- [ ] dcp_pops
Source: Download from POPs app, available on DCP Commons. Be sure to only take the public version.
*Be sure to do this source last, as the OpenData release of POPs needs to be in sync*
