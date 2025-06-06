# Equitable Development Data Explorer (EDDE)

EDDE is a set of New York City community-level data related to demographics, housing, and quality of life.

The [Racial Impact Study Coalition](https://racialimpactnyc.wordpress.com/our-story/) was the force behind it's creation.

DCP Housing is the product owner, and Data Engineering's point-person has (as of this writing in 2025) been Winnie Shen.

# The End to End Lifecycle for EDDE

### 1. Data Collection
The EDDE tool has historically been updated in June. A few months prior, typically someone in Housing reach out to other agencies
to request data for the indicators. Data Engineering is provided with a set of Excel/csv files or links to OpenData pages. 

The files we receive will map to aggregators under the /aggregate/* folder. ("aggregator" is a bit of a misnomer in the code base: input columns map pretty transparently to output columns, and unlike the typical DE data product, there isn't really any aggregation of this data)

So for example, we receive an Excel file from HPD for the Housing Vacancy Survey that contains multiple tables pertaining to housing, e.g. how many are renter-occupied, or are rent stabilized. The `housing_security/rent_stable_three_maintenance.py` aggregator translates these into four separate indicators in the app. An "indicator" typically contains multiple columns: e.g. a count, a margin of error, pcts, etc., and then potentially those same breakdowns by ethnicity.

### 2. The DE Build

#### Data Loading
Some of the data sources are links to OpenData. Loading these follows our normal process: Ingest a new version, and load via the recipe.

For data we receive in step 1 above, those files are copied into the /resources/ folder. Certain datasets are manipulated for ease of loading. (TODO: AR to add instructions on the mapping, and rename some of our files. See #1702)


#### The Build

``` shell
# runs all aggregators for housing security, housing production, and quality of life
python3 -m external_review.external_review_collate all

# runs aggregators for Economics and Demographics, which are pulled from census data. 
# There are typically three years: Census 2000, ACS previous window (e.g. 2008-2011), 
# And the ACS current window (e.g. 2019-2023)
python3 -m external_review.collate_save_census [year]
```

The outputs of this are a set files to be fed into AE's apps. They're uploaded similar to other products via our connectors.

#### The Outputs

For each of `Housing Security`, `Housing Production`, and `Quality Of Life`:
There will be one separate file per geography ['puma', 'borough', 'citywide'] per category. E.g. for `Quality of Life` we'll have:
- quality_of_life_puma.csv
- quality_of_life_borough.csv
- quality_of_life_citywide.csv

For `Economics` and `Demographics`:
One file per year per geography. e.g. In this most recent build (in 2025), there is one file for each combination of ['2000', '2008-2012', '2019-2023'] x ['puma', 'borough', 'citywide'] for 9 files each in each category. (so 18 files total for these two categories) 

In terms of input data, `Economics` and `Demographics` are the most straightforward. The data are similar to PFF: very wide tables of census data. 
By contrast, `Housing Security`, `Housing Production`, and `Quality Of Life` are messier and sometimes require processing or re-aggregating to different geographies.

### 3. Post-Build: Application Engineering's Role
AE has two processes to run to supplement the data:
1. Manually assemble the "Data Download" files. 
For any geography in the EDDE app, you can download a PDF of the Community Profile and a Data set xlsx

2. Run the [ETL here](https://github.com/NYCPlanning/ose-equity-tool-etl/) 

Additionally (and independently of DE), Pop/Housing calculate the Displacement Risk Index, which AE incoporates into the application from an Excel sheet.

### Future Work:
This product should be almost entirely declarative. The configurations in AEs repo should drive the builds. 
The AE ETL should live in data-engineering's repo, and be run with the build.

There is no codified QA process for this product. The thought previously (outside of DE) was that this product was a fairly straightforward Data In -> Data Out set of mappings, and if the source data was correct, then so too would be the outputs. There is some truth to this, but we also should add data validation that could fail a build. 

Some of this would be an easy lift: for example, there should only ever be 55 PUMAs. Any file with more or less should fail the build. 

Then there's more qualitative checks that should be written to compare a geography year over year. (Richey note: I added a qa/proto-qa.py to start in on this. I used it for validation, but it needs some work.)

## Important files

[Data Dictionary](https://www.nyc.gov/assets/planning/download/pdf/data-maps/edde/edde-data-dictionary.pdf)

## Links

[EDDE application](https://equitableexplorer.planning.nyc.gov/map/data/district)
[EDDE legislation](https://legistar.council.nyc.gov/MeetingDetail.aspx?ID=829692&GUID=2F8FEE3A-D5AE-4E32-9BF5-2D935AD6C868&Options=&Search=)

For more in-depth information, check out the data product's [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-EDDE)
