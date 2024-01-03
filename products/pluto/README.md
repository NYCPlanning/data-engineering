# Build Instructions

### I. Build PLUTO Through CI
1. Run the [Pluto Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_build.yml) action, specifying the relevant `recipe` file
- Major: `recipe.yml`
Note: You can edit the version attribute of the recipe to set the version. To generate QA against a specific previous version, set a version attribute for the `dcp_pluto` input dataset.

- Minor: `recipe-minor.yml`
Note that the Minor build will mimic the datasets from the latest release, excepting Zoning data, which will use the latest available. 

### II. Build PLUTO on Your Own Machine
In the devcontainer, run steps outlined in the [Pluto Build](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_build.yml)
Start with `Plan Build` then run steps from `Data Setup` through `Apply Corrections`. Take care to note the working directories. 

TODO: we should move the dcpy commands into shell scripts in the project, or orchestrate the entire build from dcpy commands.

## QAQC
Please refer to the [EDM QAQC web application](https://de-qaqc.nycplanningdigital.com/) for cross version comparisons


# Major Updates

## Update version name and source data versions

PLUTO uses version numbering YYvMAJOR.MINOR
1. YY for the last two digits of the release year
2. MAJOR version for using the latest versions of all input data
2. MINOR version for using the latest versions of particular input data

Make sure version name (and previous) is updated in this file
- [ ] <https://github.com/NYCPlanning/data-engineering/blob/main/products/pluto/pluto_build/version.env>

Other variables in this file are not used in a major build and can be left alone 

## Data loading

### Manual Updates

#### Updated 2x a year typically in June and December
- [ ] **dcp_colp** (check [here](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-colp.page)) 
#### Currently updated with each new release of a major version of PLUTO - important to make sure this is up to date
- [ ] **pluto_corrections** (pulling from bytes, must update when there's updates to **pluto_input_research**)

### Automated Updates

#### Open data automated pull

> Check [here](https://github.com/NYCPlanning/db-data-library/actions/workflows/open-data.yml) to see the latest run

- [ ] **dsny_frequencies**
- [ ] **dpr_greenthumb**
- [ ] **lpc_historic_districts**
- [ ] **lpc_landmarks**

#### DOF Automated Pull and Number of Buildings

- [ ] **pluto_pts** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_geocodes** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_cama_dof** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_cama.yml))
- [ ] **pluto_input_numbldgs** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_numbldgs.yml))

### Updated with Quarterly updates (check [here](https://github.com/NYCPlanning/db-data-library/actions/workflows/quaterly-updates.yml))

- [ ] **dcp_cdboundaries_wi**
- [ ] **dcp_ct2010**
- [ ] **dcp_cb2010**
- [ ] **dcp_ct2020**
- [ ] **dcp_cb2020**
- [ ] **dcp_school_districts**  
- [ ] **dcp_councildistricts_wi**  
- [ ] **dcp_firecompanies**  
- [ ] **dcp_policeprecincts**
- [ ] **dcp_healthareas**  
- [ ] **dcp_healthcenters**

### Updated with Zoning Taxlots 

(check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_dataloading.yml) for latest run).

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

### Never Updated (Safe to ignore)

- [x] **doitt_zipcodeboundaries** (almost never updated, check [here](https://data.cityofnewyork.us/Business/Zip-Code-Boundaries/i8iw-xf4u))
- [x] **fema_firms2007_100yr**
- [x] **fema_pfirms2015_100yr**
- [x] **dcp_zoningmapindex**

# Minor Updates


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

- [ ] DOF_WEEKLY_DATA_VERSION - temporarily split out to pts, numbldgs since pts temporarily fell slightly out of date. Can be rejoined to one var in minor builds of 23v3
- [ ] PLUTO_INPUT_CAMA_VERSION

- [ ] GEOSUPPORT_VERSION
- [ ] FEMA_FIRMS_VERSION
- [ ] DOITT_ZIPCODEBOUNDARIES_VERSION
- [ ] DOF_CONDO_SHORELINE_VERSION

- [ ] DCP_COLP_VERSION
- [ ] DPR_GREENTHUMB_VERSION
- [ ] DSNY_FREQUENCIES_VERSION
- [ ] LPC_HISTORIC_DISTRICTS_VERSION
- [ ] LPC_LANDMARKS_VESRSION

- [x] ~~PLUTO_CORRECTIONS_VERSION~~ defunct as of 23v2.1

## Data loading

### Updated with Zoning Taxlots 

(check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/zoningtaxlots_dataloading.yml) for latest run).

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
