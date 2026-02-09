"""Combine indicators into .csv's to be uploaded to digital ocean"""

from os import makedirs, path
from typing import Optional

import pandas as pd
import typer
from aggregate.all_accessors import Accessors

accessors = Accessors()


def collate(geography_level, category):
    """Collate indicators together"""
    accessor_functions = accessors.__getattribute__(category)
    final_df = pd.DataFrame()
    for ind_accessor in accessor_functions:
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
    folder_path = f".staging/{category}"
    if not path.exists(folder_path):
        makedirs(folder_path)
    final_df.to_csv(f".staging/{category}/{category}_{geography_level}.csv")
    return final_df


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
    for c in categories:
        for g in geographies:
            collate(g, c)


if __name__ == "__main__":
    typer.run(main)
