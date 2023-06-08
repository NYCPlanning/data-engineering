The purpose of this issue is to help the team track the tasks that need to occur before kicking off a CPDB update.

# Update source data
Read more about who to reach out to and how to update a specific dataset [here](https://github.com/NYCPlanning/db-cpdb/wiki/Maintenance)
## Received from agency partner and manually loaded into data libraries
- [ ] Capital Commitment Plan: fisa_capitalcommitments
- [ ] ddc_capitalprojects_infrastructure
- [ ] ddc_capitalprojects_publicbuildings
- [ ] dot_projects_bridges
- [ ] edc_capitalprojects - new data needs to be appended onto existing source dataset
- [ ] edc_capitalprojects_ferry - new data is often not received and this dataset is not updated with each release because nothing has changed.

## Automatically loaded into data libraries
Before initiating a build of cpdb, verify the latest version in DigitalOcean matches the version in NYC OpenData

# Projects
- [ ] [cpdb_capital_spending](https://github.com/NYCPlanning/db-cpdb/actions/workflows/spending.yml) - updated with action
- [ ] dot_projects_intersections https://data.cityofnewyork.us/Transportation/Street-and-Highway-Capital-Reconstruction-Projects/97nd-ff3i
- [ ] dot_projects_streets https://data.cityofnewyork.us/Transportation/Street-and-Highway-Capital-Reconstruction-Projects/jvk9-k4re
- [ ] dpr_capitalprojects https://www.nycgovparks.org/bigapps
- [ ] dpr_parksproperties https://data.cityofnewyork.us/Recreation/Parks-Properties/enfh-gkve
- [ ] dcp_cpdb_agencyverified (does not get updated)

# Building and lot-level info
- [ ] dcp_mappluto_wi https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-pluto-mappluto.page
- [ ] dcp_facilities https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-selfac.page
- [ ] doitt_buildingfootprints https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh

# Spatial boundaries
Come from [Geosupport update](https://github.com/NYCPlanning/db-data-library/actions/workflows/quaterly-updates.yml)
- [ ] dcp_stateassemblydistricts https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/nyad_metadata.pdf?ver=21d
- [ ] dcp_ct2020 https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_congressionaldistricts https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_cdboundaries https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_statesenatedistricts https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_municipalcourtdistricts https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_school_districts https://www1.nyc.gov/site/planning/data-maps/open-data/districts-download-metadata.page
- [ ] dcp_councildistricts https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/nycc_metadata.pdf

## Other
- [ ] dcp_trafficanalysiszones - this dataset is almost never updated 
- [ ] nypd_policeprecincts https://data.cityofnewyork.us/Public-Safety/Police-Precincts/78dh-3ptz
- [ ] fdny_firecompanies https://data.cityofnewyork.us/Public-Safety/Fire-Companies/iiv7-jaj9
