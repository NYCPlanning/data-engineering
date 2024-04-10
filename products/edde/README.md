# Equitable Development Tool

Data Repo for the [equitable development tool (EDDT)](https://legistar.council.nyc.gov/MeetingDetail.aspx?ID=829692&amp;GUID=2F8FEE3A-D5AE-4E32-9BF5-2D935AD6C868&amp;Options=&amp;Search=)

# Building
There are five output folders/categories, the first two regarding the census
- demographics
- economics
- housing_production
- housing_security
- quality_of_life
For each of these, exports are made by geography: "citywide", "borough", or "puma".

For census data, we export by year, exporting for year 2000 and each year we have ACS data for.

For the other three categories, we create one export per publishing (per geography). Some are based on the latest data, some are pulling both latest data and some specific older version(s) for comparison.

## Command line 
eddt.sh is a simple entry point to the functionality in this repo. There are three valid primary commands
- `dataloading` - loads some data from data library (most is kept locally in this repo)
- `build` - builds data and dumps output into 
  - with no additional arguments, builds all subcategories 
  - first optional positional argument: `census` or `category`. Either of these supplied on there own will call underlying python script with no arguments (external_review/collate_save_census.py and external_review_collate.py). These two scripts have optional arguments as well, which can be added on to this script call. For census, they are
    - `year`, `category`, and `geography`. Year can be any ACS year range (i.e. "0812", "1519"). Category can be "economics" or "demographics". Any of them can be "all" (blank is read as all as well, but in cases where say all years are wanted but specific geography, you'd need `eddt.sh build census all all borough`)
  for `category`, subcategory and year are possible arguments (subcategories being `housing_production`, etc.)
- `export` - exports to Digital Ocean (requires local secrets) from `.staging` folder. Optional arg is the export folder (demographics, economics, housing_production, etc.). With no arg, will export all folders in .staging

## Github Actions
Workflows can be run in github actions manually via workflow dispatch. Census export does both census categories for all years, while the "category" export offers choice of categories, or all