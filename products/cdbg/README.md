# Community Development Block Grant (CDBG)

The Community Development Block Grant (CDBG) data product determines, for all census block groups and tracts in New York City, if activites in an area of the city qualify for funding through the CDBG program.

NYC receives Community Development Block Grant funds from the U.S. Department of Housing and Urban Development (HUD). These funds may be used for projects in residential areas where over half of the population is considered low- and moderate-income. Income data is proved by HUD as the [Low- and Moderate-Income Summary Data (LMISD)](https://www.hudexchange.info/programs/cdbg/cdbg-low-moderate-income-data/).

A census tract is considered residential if at least 50.0% of the total built floor area is residential. This is determined by summing ResArea and BldgArea for MapPLUTO lots in the census tract. If at least 90% of a lot's area is in the tract, the entire lot is assigned to that tract. Otherwise, the building area is assigned proportionately based on the percent of the lot's area in each census tract.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/cdbg/recipe.yml)

## Links

[CDBG Overview on DCP website](https://www.nyc.gov/site/planning/data-maps/community-development-block-grant.page)
[CDBG on Bytes](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-cdbg.page)
