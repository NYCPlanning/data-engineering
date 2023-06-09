---
name: PLUTO Build - Major
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

Make sure version name (and previous) is updated in this file
- [ ] <https://github.com/NYCPlanning/db-pluto/blob/main/pluto_build/version.env>

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

- [ ] **pluto_pts** (Check [here](https://github.com/NYCPlanning/db-pluto/actions/workflows/input_pts.yml))
- [ ] **pluto_input_geocodes** (Check [here](https://github.com/NYCPlanning/db-pluto/actions/workflows/input_pts.yml))
- [ ] **pluto_input_cama_dof** (Check [here](https://github.com/NYCPlanning/db-pluto/actions/workflows/input_cama.yml))
- [ ] **pluto_input_numbldgs** (Check [here](https://github.com/NYCPlanning/db-pluto/actions/workflows/input_numbldgs.yml))

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

### Never Updated (Safe to ignore)

- [x] **doitt_zipcodeboundaries** (almost never updated, check [here](https://data.cityofnewyork.us/Business/Zip-Code-Boundaries/i8iw-xf4u))
- [x] **fema_firms2007_100yr**
- [x] **fema_pfirms2015_100yr**
- [x] **dcp_zoningmapindex**

### Update QAQC App 

~~- [ ] **update data-engineering-qaqc version_comparison_report in src/pluto/pluto.py with version of pluto build** (must be updated with every version of pluto [here] (https://github.com/NYCPlanning/data-engineering-qaqc/blob/main/src/pluto/pluto.py))~~

# PLUTO Improvements (corrections)

# Build CI Runs

- **[INSERPT LINKS TO RELEVANT CI RUNS HERE]**
 
# Comments

