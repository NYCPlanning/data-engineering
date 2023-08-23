---
name: PLUTO Build - Minor
about: This is a build issue for PLUTO that tracks dataloading and build
title: "PLUTO build version: {e.g. 20v4, 22v3.1}"
labels: ["pluto", "data update"]
assignees:

---

## Update version name and source data versions

PLUTO uses version numbering YYvMAJOR.MINOR
1. YY for the last two digits of the release year
2. MAJOR version for using the latest versions of all input data
2. MINOR version for using the latest versions of particular input data

Make sure version name is updated in this file
- [ ] <https://github.com/NYCPlanning/data-engineering/blob/main/products/pluto/pluto_build/version.env>

In that file, the following are also specified. These versions are to be help constant with what was used in the last major pluto update.
They are not used by the main/major data loading script, so they are only updated at the first minor release of each major release.
For example, say 22v1.4 is the last release. 22v2 is to be built. These args are not used in the build process, so they do not need to be updated.
When it's time to build 22v2.1, these must be updated to match [the versions used in 22v2](https://nyc3.digitaloceanspaces.com/edm-publishing/db-pluto/22v2/latest/output/source_data_versions.csv) as they were last used for 22v1 minor versions. Finally, when 22v2.2 is being built, they don't need to be updated since they've
already been updated to be set to 22v2 versions

- [ ] DOF_WEEKLY_DATA_VERSION
- [ ] DOF_CAMA_DATA_VERSION

- [ ] GEOSUPPORT_VERSION
- [ ] FEMA_FIRPS_VERSION
- [ ] DOITT_DATA_VERSION
- [ ] DOF_DATA_VERSION

- [ ] DCP_COLP_VERSION
- [ ] DPR_GREENTHUMB_VERSION
- [ ] DSNY_FREQUENCIES_VERSION
- [ ] LPC_HISTORIC_DISTRICTS_VERSION
- [ ] LPC_LANDMARKS_VESRSION

- [ ] PLUTO_CORRECTIONS_VERSION

## Data loading

### Updated with Zoning Taxlots 

(check [here](https://github.com/NYCPlanning/db-zoningtaxlots/actions/workflows/dataloading.yml) for latest run).

These are all produced by GIS, who typically update them sometime in the first week of each month.
Check in with them before archiving with data library

- [ ] **dof_dtm**
- [ ] **dof_shoreline**
- [ ] **dof_condo**
- [ ] **dcp_commercialoverlay**
- [ ] **dcp_limitedheight**
- [ ] **dcp_zoningdistricts**
- [ ] **dcp_specialpurpose**
- [ ] **dcp_specialpurposesubdistricts**
- [ ] **dcp_zoningmapamendments**
- [ ] **dcp_edesignation**

### Update QAQC App 

~~- [ ] **update data-engineering-qaqc version_comparison_report in src/pluto/pluto.py with version of pluto build** (must be updated with every version of pluto [here] (https://github.com/NYCPlanning/data-engineering-qaqc/blob/main/src/pluto/pluto.py))~~

## Build CI Runs

- **[INSERPT LINKS TO RELEVANT CI RUNS HERE]**
 
## Comments

