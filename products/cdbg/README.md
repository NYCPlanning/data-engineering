# Community Development Block Grant (CDBG)

The Community Development Block Grant (CDBG) dataset determines, for all census block groups and tracts in New York City, if activities in an area of the city qualify for funding through the CDBG program.

NYC receives CDBG funds from the U.S. Department of Housing and Urban Development (HUD). These funds may be used for projects in residential areas where over half of the population is considered low- and moderate-income. Income data is proved by HUD as the [Low- and Moderate-Income Summary Data (LMISD)](https://www.hudexchange.info/programs/cdbg/cdbg-low-moderate-income-data/).

Rather than total population, the number of people with the potential for being deemed low- and moderate-income is used to determine an areas eligibility. This ensures alignment with HUD's income percentage determinations. HUD use the Census Bureau's definition of persons eligible, which removes persons in group housing such as college students, jails and nursing homes. For more information, visit HUD's website.

A census tract is considered residential if at least 50.0% of the total built floor area is residential. This is determined by summing ResArea and BldgArea for MapPLUTO lots in the census area. If at least 90% of a lot's area is in the tract, the entire lot is assigned to that census area. Otherwise, the building area is assigned proportionately based on the percent of the lot's area in each census area.

## Important files

[recipe](https://github.com/NYCPlanning/data-engineering/blob/main/products/cdbg/recipe.yml)

## Links

[CDBG Overview on DCP website](https://www.nyc.gov/site/planning/data-maps/community-development-block-grant.page)
[CDBG on Bytes](https://www.nyc.gov/site/planning/data-maps/open-data/dwn-cdbg.page)
