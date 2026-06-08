"""Build EDDE indicators and save to CSV files.

Combines census/ACS data (demographics, economics) with other indicators
(housing_security, housing_production, quality_of_life).
"""

import shutil
from typing import Optional

import pandas as pd
import typer
from aggregate.all_accessors import Accessors
from aggregate.decennial_census.decennial_census_001020 import decennial_census_001020
from aggregate.load_aggregated import initialize_dataframe_geo_index
from aggregate.PUMS.pums_2000_demographics import pums_2000_demographics
from aggregate.PUMS.pums_2000_economics import pums_2000_economics
from aggregate.PUMS.pums_demographics import acs_pums_demographics
from aggregate.PUMS.pums_economics import acs_pums_economics
from config import PRODUCT_PATH, clean_build_output_dir, get_build_output_dir
from utils.geo_helpers import acs_years

from dcpy.lifecycle.builds import (
    get_build_metadata_path,
    get_recipe_lock,
    get_recipe_lock_path,
)
from dcpy.utils.logging import logger

accessors = Accessors()


class CensusAccessors:
    """Census/ACS data accessors for demographics and economics.

    All function calls return iterables for consistency.
    """

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


def get_ignored_indicators() -> set[str]:
    """Get list of ignored indicators from recipe lock file."""
    try:
        recipe_lock = get_recipe_lock(PRODUCT_PATH)
        if recipe_lock.custom and "ignored_indicators" in recipe_lock.custom:
            ignored = set(recipe_lock.custom["ignored_indicators"])
            logger.info(f"Loaded {len(ignored)} ignored indicators: {ignored}")
            return ignored
    except Exception as e:
        logger.warning(f"Could not load ignored indicators from recipe lock: {e}")
    return set()


def build_census_category(
    eddt_category: str,
    geography: str,
    year: str,
) -> pd.DataFrame:
    """Build census/ACS indicators for demographics or economics.

    Args:
        eddt_category: "demographics" or "economics"
        geography: "citywide", "borough", or "puma"
        year: Year string (e.g., "2000", "0812", "1923")

    Returns:
        DataFrame with all indicators for the category/geography/year
    """
    logger.info(f"Building census data: {eddt_category}, {geography}, {year}")
    final = initialize_dataframe_geo_index(geography=geography)

    # Determine accessor suffix based on year
    if year == "2000":
        suffix = year
    else:
        suffix = "generic"

    # Get and execute accessors
    for accessor in getattr(CensusAccessors, f"{eddt_category}_{suffix}")():
        df = accessor(geography, year)
        final = final.merge(df, left_index=True, right_index=True)

    # Save to output directory
    build_output_dir = get_build_output_dir()
    data_dir = build_output_dir / "data"
    folder_path = data_dir / eddt_category
    folder_path.mkdir(parents=True, exist_ok=True)
    output_file = folder_path / f"{eddt_category}_{year}_{geography}.csv"
    logger.info(
        f"Building {eddt_category} indicators for {year} {geography}, saving to: {output_file}"
    )
    final.to_csv(output_file)
    return final


def build_other_category(geography: str, category: str) -> pd.DataFrame:
    """Build non-census indicators (housing_security, housing_production, quality_of_life).

    Args:
        geography: "citywide", "borough", or "puma"
        category: "housing_security", "housing_production", or "quality_of_life"

    Returns:
        DataFrame with all indicators for the category/geography
    """
    logger.info(f"Building {category} indicators for {geography}")

    # Load ignored indicators from recipe lock
    ignored_indicators = get_ignored_indicators()

    accessor_functions = accessors.__getattribute__(category)
    final_df = pd.DataFrame()

    for ind_accessor in accessor_functions:
        indicator_name = ind_accessor.__name__

        # Skip ignored indicators
        if indicator_name in ignored_indicators:
            logger.info(f"Skipping ignored indicator: {indicator_name}")
            continue

        try:
            print(f"calculating {ind_accessor.__name__}")
            ind = ind_accessor(geography)
            if final_df.empty:
                final_df = ind
            else:
                final_df = final_df.merge(
                    ind, right_index=True, left_index=True, how="left"
                )
        except Exception as e:
            print(
                f"Error merging indicator {ind_accessor.__name__} at geography level {geography}"
            )
            raise e

    final_df.index.rename(geography, inplace=True)

    # Save to output directory
    build_output_dir = get_build_output_dir()
    data_dir = build_output_dir / "data"
    folder_path = data_dir / category
    folder_path.mkdir(parents=True, exist_ok=True)
    output_file = folder_path / f"{category}_{geography}.csv"
    logger.info(
        f"Building {category} indicators for {geography}, saving to: {output_file}"
    )
    final_df.to_csv(output_file)
    return final_df


def copy_build_metadata():
    """Copy recipe.lock.yml and build_metadata.json to build output directory."""
    build_output_dir = get_build_output_dir()

    # Copy recipe.lock.yml
    recipe_lock_source = get_recipe_lock_path(PRODUCT_PATH)
    recipe_lock_dest = build_output_dir / "recipe.lock.yml"
    if recipe_lock_source.exists():
        shutil.copy2(recipe_lock_source, recipe_lock_dest)
        logger.info(f"Copied recipe.lock.yml to {recipe_lock_dest}")
    else:
        logger.warning(f"recipe.lock.yml not found at {recipe_lock_source}")

    # Copy build_metadata.json
    build_metadata_source = get_build_metadata_path(PRODUCT_PATH)
    build_metadata_dest = build_output_dir / "build_metadata.json"
    if build_metadata_source.exists():
        shutil.copy2(build_metadata_source, build_metadata_dest)
        logger.info(f"Copied build_metadata.json to {build_metadata_dest}")
    else:
        logger.warning(f"build_metadata.json not found at {build_metadata_source}")


def main(
    category: Optional[str] = typer.Argument(None),
    geography: Optional[str] = typer.Argument(None),
    year: Optional[str] = typer.Argument(None),
):
    """Build EDDE indicators.

    Args:
        category: Category to build (demographics, economics, housing_security,
                 housing_production, quality_of_life) or "all"
        geography: Geography level (citywide, borough, puma) or "all"
        year: Year for census categories (2000, 0812, 1923, etc.) or "all"
    """

    def assert_opt(arg, options):
        assert (arg is None) or (arg == "all") or (arg in options)

    # Census categories (require year parameter)
    census_categories = ["demographics", "economics"]
    # Other categories (don't use year parameter)
    other_categories = ["housing_security", "housing_production", "quality_of_life"]
    all_categories = census_categories + other_categories

    geographies = ["citywide", "borough", "puma"]
    years = acs_years + ["2000"]

    assert_opt(category, all_categories)
    assert_opt(geography, geographies)
    assert_opt(year, years)

    # Determine which categories to build
    if category is None or category == "all":
        categories_to_build = all_categories
    else:
        categories_to_build = [category]

    # Determine which geographies to build
    if geography is None or geography == "all":
        geographies_to_build = geographies
    else:
        geographies_to_build = [geography]

    # Determine which years to build (for census categories)
    if year is None or year == "all":
        years_to_build = years
    else:
        years_to_build = [year]

    logger.info(
        f"Building: {categories_to_build} × {geographies_to_build}"
        + (
            f" × {years_to_build}"
            if any(c in census_categories for c in categories_to_build)
            else ""
        )
    )

    # Clean category directories before starting build
    clean_build_output_dir(categories_to_build)

    # Build census categories
    for cat in categories_to_build:
        if cat in census_categories:
            for geo in geographies_to_build:
                for yr in years_to_build:
                    build_census_category(cat, geo, yr)

    # Build other categories
    for cat in categories_to_build:
        if cat in other_categories:
            for geo in geographies_to_build:
                build_other_category(geo, cat)

    # Copy metadata files to build output directory
    copy_build_metadata()


if __name__ == "__main__":
    typer.run(main)
