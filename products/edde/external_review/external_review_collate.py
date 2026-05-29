"""Combine indicators into .csv's to be uploaded to digital ocean"""

import shutil
from typing import Optional

import pandas as pd
import typer
from aggregate.all_accessors import Accessors
from config import PRODUCT_PATH, clean_build_output_dir, get_build_output_dir

from dcpy.lifecycle.builds import (
    get_build_metadata_path,
    get_recipe_lock,
    get_recipe_lock_path,
)
from dcpy.utils.logging import logger

accessors = Accessors()


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


def collate(geography_level, category):
    """Collate indicators together"""
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
            ind = ind_accessor(geography_level)
            if final_df.empty:
                final_df = ind
            else:
                final_df = final_df.merge(
                    ind, right_index=True, left_index=True, how="left"
                )
        except Exception as e:
            print(
                f"Error merging indicator {ind_accessor.__name__} at geography level {geography_level}"
            )
            raise e
    final_df.index.rename(geography_level, inplace=True)
    build_output_dir = get_build_output_dir()
    data_dir = build_output_dir / "data"
    folder_path = data_dir / category
    folder_path.mkdir(parents=True, exist_ok=True)
    output_file = folder_path / f"{category}_{geography_level}.csv"
    logger.info(
        f"Building {category} indicators for {geography_level}, saving to: {output_file}"
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
    eddt_category: Optional[str] = typer.Argument(None),
    geography: Optional[str] = typer.Argument(None),
):
    def assert_opt(arg, list):
        assert (arg is None) or (arg == "all") or (arg in list)

    categories = ["housing_security", "housing_production", "quality_of_life"]
    geographies = ["citywide", "borough", "puma"]
    assert_opt(eddt_category, categories)
    assert_opt(geography, geographies)

    if eddt_category is not None and eddt_category != "all":
        categories = [eddt_category]
    if geography is not None and geography != "all":
        geographies = [geography]

    # Clean category directories before starting build
    clean_build_output_dir(categories)

    for c in categories:
        for g in geographies:
            collate(g, c)

    # Copy metadata files to build output directory
    copy_build_metadata()


if __name__ == "__main__":
    typer.run(main)
