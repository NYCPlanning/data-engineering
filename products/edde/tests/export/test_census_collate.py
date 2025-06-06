"""EDDT uses three census surveys. ACS PUMS, census PUMS, and the census decennial.
ACS PUMS data comes from the MDAT API.
The census decennial and census PUMS currently come from spreadsheets prepated by DCP population.
These data are combined during the export process where files by geography-year are sent
to digital ocean. These files should be as consistent as possible, using similar column names,
column orders, etc.
"""

from external_review.collate_save_census import collate_save_census
from utils.geo_helpers import acs_years

save_kwargs = {
    "eddt_category": "demographics",
    "geography": "puma",
}
demos_2000 = collate_save_census(year="2000", **save_kwargs)

## thought here is to keep 0812/1519 for stability of code - if something is broken, reflect in data set where we know things should be consistent
## then also test latest more as a test of stability of data
demos_0812 = collate_save_census(year="0812", **save_kwargs)
demos_1519 = collate_save_census(year="1519", **save_kwargs)
demos_latest = collate_save_census(year=acs_years[-1], **save_kwargs)


def test_matching_column_orders_acs_demographics_0812_1519():
    assert (demos_0812.columns == demos_1519.columns).all()


def test_matching_column_orders_acs_demographics_0812_latest():
    assert (demos_0812.columns == demos_latest.columns).all()


def test_matching_column_orders_demographics():
    """The only differences should be the denom columns. Filter those out"""
    demos_2000_no_denom = demos_2000[
        [c for c in demos_2000.columns if "age_p5pl" not in c]
    ]

    demos_0812_no_denom = demos_0812[
        [c for c in demos_0812.columns if "denom" not in c]
    ]

    demos_latest_no_denom = demos_latest[
        [c for c in demos_latest.columns if "denom" not in c]
    ]

    assert (demos_2000_no_denom.columns == demos_0812_no_denom.columns).all()
    assert (demos_2000_no_denom.columns == demos_latest_no_denom.columns).all()
