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
- [ ] data packaged and distributed (including DOF sftp)
- [ ] shared QA reports with AD

## Data loading

### Manual Updates

#### Updated 2x a year typically in June and December
- [ ] **dcp_colp** (check [here](https://www1.nyc.gov/site/planning/data-maps/open-data/dwn-colp.page)) 

### Automated Updates

#### Open data automated pull

> Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/ingest_open_data.yml) to see the latest run

- [ ] **dsny_frequencies**
- [ ] **dpr_greenthumb**
- [ ] **lpc_historic_districts**
- [ ] **lpc_landmarks**

#### DOF Automated Pull and Number of Buildings

- [ ] **pluto_pts** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_geocodes** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_pts.yml))
- [ ] **pluto_input_cama_dof** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_cama.yml))
- [ ] **pluto_input_numbldgs** (Check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/pluto_input_numbldgs.yml))

### Updated with Quarterly updates (check [here](https://github.com/NYCPlanning/data-engineering/actions/workflows/ingest_bytes_quarterly.yml))

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


### QA Checklist

**1. Data Extraction**

TODO: This should become a data check 

* Ensure zoning data matches the build month
* Confirm Geosupport version aligns with PLUTO version (e.g., 25a for 24Q1)

**2. Data Quality Review**

We resolve all data quality issues before promoting PLUTO to drafts. These may come from:

* Sub-issues (GitHub or email)
* Failed dbt tests during build
* PLUTO QAQC page

**Actions:**

* **Investigate stakeholder issues**

  * Check if the field in question has ever been corrected (via `pluto_input_research.csv`)
  * If no past corrections for this field: notify the data provider (e.g., DOF) and inform the requester that DCP doesn't correct this field
  * If yes, add a manual correction

* **Investigate failed dbt checks**

  * Refer to data check descriptions in the repo
  * Manually correct records or add to an ignore seed file to pass check

* **Investigate QAQC page (e.g., unreasonably small apartments)**

  * Use provided guidance on the QAQC page to review and correct records as needed.

**3. After publishing PLUTO**

Send the following to AD:

* `pts_condo_units_{{version}}.csv`: all BBLs with active `resunits` corrections in PLUTO
* `devdb_condo_units_{{version}}.csv`: same BBLs joined to devdb for unit count comparison
* Zip and send via email with a PTS data version noted
* AD will forward these reports to DOF for them to review and address
