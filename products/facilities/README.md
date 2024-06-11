# Facilities database (FacDB)

The City Planning Facilities Database aggregates more than 35,000 records from 52 different public data sources provided by City, State, and Federal agencies.

While each source agency classifies its facilities according to their own naming systems, we have grouped all facilities and program sites into the following seven categories to help planners navigate the data more easily:

* Health and Human Services
* Education, Child Welfare, and Youth
* Parks, Gardens, and Historical Sites
* Libraries and Cultural Programs
* Public Safety, Emergency Services, and Administration of Justice
* Core Infrastructure and Transportation
* Administration of Government

Within each of these domains, each record is further categorized into a set of facility groups, subgroups, and types that are intended to make the data easy to navigate and more useful for specific planning purposes. Facility types and names appear as they do in source datasets, wherever possible. A full listing of the facility categories is provided in the data dictionary.

### Important files

 ```bash
facilities/
├── facdb/
│   ├── bash/
│   ├── data/
│   ├── geocode/
│   ├── sql/ # scripts with transformation logic
│   ├── utility/
│   ├── __init__.py
│   ├── cli.py # entry point to run build steps
│   ├── datasets.yml
│   ├── metadata.yml
│   └── pipelines.py
├── README.md # this file
├── facdb.sh # script to export from database and upload artifacts
├── recipe.yml # list of input datasets, AKA "recipes"
└── version.env # file containing previous FacDB version
 ```

Additionally:

[`metadata`](https://github.com/NYCPlanning/product-metadata/blob/main/products/facilities/facilities/metadata.yml)

[`Github workflow`](https://github.com/NYCPlanning/data-engineering/blob/main/.github/workflows/facilities_build.yml) - config file used to build Facilities via Github Actions

### Links

[FacDB on Bytes](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-selfac.page)  

[FacDB on OpenData](https://data.cityofnewyork.us/City-Government/Facilities-Database/ji82-xba5/about_data)

For more in-depth information on FacDB and its schema, check out its [wiki page](https://github.com/NYCPlanning/data-engineering/wiki/Product:-FacDB).
