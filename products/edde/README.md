# Equitable Development Data Explorer (EDDE)

EDDE is a set of New York City community-level data related to demographics, housing, and quality of life.


# The End to End Lifecycle for EDDE

### 1. Data Collection
The EDDE tool has historically been updated in June. A few months prior, Population and/or Housing reach out to other agencies
to request data for the indicators. Data Engineering is provided with a set of Excel/csv files. These files map to aggregators 
under the /aggregate/* folder. Unlike the typical DE data product, there isn't any aggregating of this data - In general, input columns
map pretty transparently to output columns. 

Additionally (and independently of us), Pop/Housing calculate the Displacement Risk Index, which AE incoporates into the application from an Excel sheet.

### 2. The DE Build

``` shell
# runs all aggregators for housing security, housing production, and quality of life
python3 -m external_review.external_review_collate all

# runs aggregators for Economics and Demographics, which are pulled from census data. 
# There are typically three years: Census 2000, ACS previous window (e.g. 2008-2011), 
# And the ACS current window (e.g. 2019-2023)
python3 -m external_review.collate_save_census [year]
```

The outputs of this are a set files to be fed into AE's apps. 

For `Housing Security`, `Housing Production`, and `Quality Of Life`:
One a separate file per geography ['puma', 'borough', 'citywide']

for `Economics` and `Demographics`:
One file per year per geography. e.g. One file for each combination of ['2000', '2008-2012', '2019-2023'] x ['puma', 'borough', 'citywide'] for 9 files each. 


### 3. The AE ETL
AE has two processes to run:
1. Manually assemble the "Data Download" files. 
For any geography in the EDDE app, you can download a PDF of the Community Profile and a Data set xlsx

2. Run the [ETL here](https://github.com/NYCPlanning/ose-equity-tool-etl/) 

### Future Work:
This product should be almost entirely declarative. The configurations in AEs repo should drive the builds. 

## Important files

[Data Dictionary](https://www.nyc.gov/assets/planning/download/pdf/data-maps/edde/edde-data-dictionary.pdf)

## Links

[EDDE application](https://equitableexplorer.planning.nyc.gov/map/data/district)
[EDDE legislation](https://legistar.council.nyc.gov/MeetingDetail.aspx?ID=829692&GUID=2F8FEE3A-D5AE-4E32-9BF5-2D935AD6C868&Options=&Search=)

For more in-depth information, check out the data product's [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-EDDE)
