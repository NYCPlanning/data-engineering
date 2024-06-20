---
name: "Product Update: Pluto Major"
about: "Product Update: Pluto Major"
title: "Product Update: Pluto {{ VERSION }}"
labels: 'data update'
---

## Main tasks

- [ ] source data extracted
- [ ] draft build succeeded
- [ ] draft build passed QA
- [ ] data packaged and distributed

## Data loading

### Manual Updates

#### Updated 2x a year typically in June and December
- [ ] **dcp_colp** (check [here](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-colp.page)) 

### Automated Updates

#### Open data automated pull

> Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/data_library_open_data.yml) to see the latest run

- [ ] **dsny_frequencies**
- [ ] **dpr_greenthumb**
- [ ] **lpc_historic_districts**
- [ ] **lpc_landmarks**

#### DOF Automated Pull and Number of Buildings

- [ ] **pluto_pts** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_geocodes** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_cama_dof** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_cama.yml))
- [ ] **pluto_input_numbldgs** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_numbldgs.yml))

### Updated with Quarterly updates (check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/data_library_quarterly.yml))

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
