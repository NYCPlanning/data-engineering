"""This functionality should eventually be in the same file as external_review_collate
where housing production is collated and saved. Up against deadline it's easier to
write a new file but this step can be DRY'd out and brought down to a simplier format"""

from dcpy.utils.logging import logger
from os import path, makedirs
import typer
from typing import Optional

from aggregate.decennial_census.decennial_census_001020 import decennial_census_001020
from aggregate.PUMS.pums_2000_demographics import pums_2000_demographics
from aggregate.PUMS.pums_2000_economics import pums_2000_economics
from aggregate.PUMS.pums_demographics import acs_pums_demographics
from aggregate.PUMS.pums_economics import acs_pums_economics
from utils.PUMA_helpers import acs_years


from aggregate.load_aggregated import initialize_dataframe_geo_index


def collate_save_census(
    eddt_category,
    geography,
    year,
):
    logger.info(f"Running {eddt_category}, {geography}, {year}")
    final = initialize_dataframe_geo_index(geography=geography)
    if year == "2000":
        suffix = year
    else:
        suffix = "generic"
    for accessor in getattr(CensusAccessors, f"{eddt_category}_{suffix}")():
        df = accessor(geography, year)
        final = final.merge(df, left_index=True, right_index=True)

    folder_path = f".staging/{eddt_category}"
    if not path.exists(folder_path):
        makedirs(folder_path)
    final.to_csv(f".staging/{eddt_category}/{eddt_category}_{year}_{geography}.csv")
    return final


class CensusAccessors:
    """All function calls return iterables for consistency."""

    @classmethod
    def demographics_2000(cls):
        return [decennial_census_001020, pums_2000_demographics]

    @classmethod
    def economics_2000(cls):
        return [pums_2000_economics]

    @classmethod
    def demographics_generic(cls):
        return [decennial_census_001020, acs_pums_demographics]

    @classmethod
    def economics_generic(cls):
        return [acs_pums_economics]


def main(
    year: Optional[str] = typer.Argument(None),
    eddt_category: Optional[str] = typer.Argument(None),
    geography: Optional[str] = typer.Argument(None),
):
    def assert_opt(arg, list):
        assert (arg is None) or (arg == "all") or (arg in list)

    categories = ["economics", "demographics"]
    geographies = ["citywide", "borough", "puma"]
    years = acs_years
    years.append("2000")
    assert_opt(year, years)
    assert_opt(eddt_category, categories)
    assert_opt(geography, geographies)

    if year is not None and year != "all":
        years = [year]
    if eddt_category is not None and eddt_category != "all":
        categories = [eddt_category]
    if geography is not None and geography != "all":
        geographies = [geography]

    logger.info(f"Running Census/ACS for: {year} years x {geographies} x {categories}")
    for c in categories:
        for g in geographies:
            for y in years:
                collate_save_census(c, g, y)


if __name__ == "__main__":
    typer.run(main)
