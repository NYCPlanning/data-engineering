# db-factfinder

## Ingesting Data Sources

We need to ingest a few data sources for this dataset, for both ACS and Census. This is a somewhat unique 
product in that Population will hand us corrections to datasets from previous years, and those changes 
are integrated into recipes. 

#### Census
Unless you're reading this in the year 2031, you probably don't need to ingest a new census. 

However, there may be fixes/corrections/updates to census data from previous years. 
For example, in the most recent update, we we're given a file called `Select2010DataForRefresh_for1923Update.xlsx`

#### ACS



## Running the Build
Code in this repo accomplishes a few things
- Pulling data from the Census Bureau and doing statistical calculations.
  This is not applicable if DCP Population hands us files, as they have done in the 2019-2023 update.
  It should eventually be refactored out into a `dcpy` connector. And the census data it pulls is really more akin
  to a data product.
  
Pipelines to build Decennial and Survey inputs to the PFF app. 
(TODO: add product-metadata for PFF)
  

## TLDR / Cheatsheet On The Data Sources 




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

The brackets (e.g. [census]) indicate which datasets apply to which geotypes in the PFF app.
e.g. for Census Block, only census data is available, but not ACS data.

##### Geo types From the Census Bureau:
- CB (Census Block)
  An actual city block.
  [census] 
  (*note that for CB we don't have "detailed" census data. "detailed" refers to ethnic breakdown)

- CT
  collection of CBs, ranging from 1.2k to 8k people. When they get too large/small, they're divided or merged
  [census, ACS]
  
- NTA
  Approximation of a neighborhood (e.g. Park Slope) built out of Census Tracts. 
  [census, ACS]

- CDTA: 
  approximation of a CD, composed of NTAs.
  [census, ACS]

So All together, the building blocks are:
- CDTA > NTA > CT > CB
  (where ">" means "is composed of")

##### Geo types From NYC:
- CCD (city council districts)
  Redrawn each decade for census. 
  [census, ACS]

- CD (community district)
  Similar in size/location to CCD, but are effectively unchanging (at least since 1975)
  [census (non-detailed, similar to CB)] 

##### Geo types From Both:
City and Borough

