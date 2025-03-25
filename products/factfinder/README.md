# db-factfinder


## Overview: Outputs
Factfinder outputs four datasets
1. 2010 Census
2. 2020 Census
3. ACS Current:  a current five-year window of the ACS (e.g. 2019-2023), 
4. ACS Previous: a benchmark/comparison window from the past (e.g. 2006-2010)

Those datasets also include product metadata, effectively a data dictionary. 

## Build Instructions

### 1. Ingest
We need to ingest a few data sources for this dataset, for both ACS and Census. This is a somewhat unique 
product in that Population will hand us corrections to datasets from previous years, and those changes 
are integrated into recipes. There are two types of dataset that might require new ingested versions:

#### Census
Unless you're reading this in the year 2031, you probably don't need to create an entirely new census dataset to ingest. 
You'll need to ingest updated 2020 Census data though.

Also, there may be fixes/corrections/updates to census data from 2020. 
For example, in the most recent update, we were given a file called `Select2010DataForRefresh_for1923Update.xlsx`
containing corrections to the census 2010 dataset. In such cases, you should take pull the existing 2010 data in recipes, amend(*) it, and ingest this new version.

Note: the corrections could be either for 2010 dhc or the ddhc. Make sure you apply them in the right file!

(*) There's not a set-in-stone way to amend these datasets. (AR note: in 2025, I used the DCPY excel utils to do a cross-file vlookup)

#### ACS
Similar to the Census, there may be corrections to the previous window of the ACS data. In this case, it's slightly more complicated in that 
1) new variables may be introduced in the current window (e.g. 2019-2023) that are not present in the previous, and
2) we have four recipes for previous-window (e.g. 2006-10) ACS data, one recipe for each of the census categories of demography, housing, social and economic.

In the 2024 update, for example, we were given a file with new variables for housing for 2006-10. In such a case, we need to 
1) determine which previous-window file the new variables belong in
2) add them there
3) ingest the updated data.

## Running the Build
Code in this repo primarily:
1. Pulls data from the Census Bureau and doing statistical calculations.
   This is not applicable if DCP Population hands us files, as they have done in the 2019-2023 update.
  
2. Runs pipelines to build Decennial and ACS datasets. 
   You'll probably want to run both `products/factfinder/run.py` for both 'acs' and 'decennial'


## Cheatsheet On The Data Sources 

### ACS (American Community Survey)
- conducted every month, sent to ~3.5 million people
- goes deeper than the decennial (e.g. education qs)
- DCP Population maintains two rolling 5-year-average datasets, one for new data (e.g. 2019-2023)
  and another for reference from the last century, 2006-2010. These are used as the comparison 

### Census (AKA the decennial)
- conducted every decade
- targets every household in the US
- Drives the mapping of all the Geo types from the census below. 

We maintain two recipes:
- `dcp_pop_decennial_ddhca`: the extra "d" means "detailed", specifically referring to ethnic breakdowns. This data is not available at the CB level.
- `dcp_pop_decennial_dhc`: the rest of the census data

Note that both files contain data aggregated at non-Census geotypes, CCD and CD. Specifically, 
- ddhca has `CCD`s
- dhc has both `CCD`s and `CD`s

TODO: figure out why they're different


## TLDR / Cheatsheet for Geotypes in the PFF app

The brackets, e.g. [census], indicate which datasets apply to which geotypes in the PFF app.
e.g. for Census Block, only census data is available, but not ACS data.

### Geo types From the Census Bureau:

#### CB (Census Block)
An actual city block.
[census] 
(*note that for CB we don't have "detailed" census data. "detailed" refers to ethnic breakdown)

#### CT
collection of CBs, ranging from 1.2k to 8k people. When they get too large/small, they're divided or merged
[census, ACS]

#### NTA
Approximation of a neighborhood (e.g. Park Slope) built out of Census Tracts. 
[census, ACS]

#### CDTA: 
approximation of a CD, composed of NTAs.
[census, ACS]


So All together, the building blocks are:
#### CDTA > NTA > CT > CB
(where ">" means "is composed of")

### Geo types From NYC:
#### CCD (city council districts)
Redrawn each decade for census. 
[census, ACS]

#### CD (community district)
Similar in size/location to CCD, but are effectively unchanging (at least since 1975)
[census (non-detailed, similar to CB)] 

### Geo types From Both:
City and Borough

