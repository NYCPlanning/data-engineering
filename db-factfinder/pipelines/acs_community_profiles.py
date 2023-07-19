import argparse
import json
import os
import sys
from functools import reduce
from typing import Tuple

import pandas as pd
from pathos.pools import ProcessPool

from factfinder.calculate import Calculate

from . import API_KEY


def parse_args() -> Tuple[int, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y", "--year", type=int, help="The ACS5 year, e.g. 2019 (2014-2018)"
    )
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.year, args.geography


with open("pipelines/acs_community_profiles_variable_mapping.json", "r") as f:
    acs_variables = json.load(f)

if __name__ == "__main__":
    # Get ACS year
    year, geography = parse_args()
    pool = ProcessPool(nodes=10)

    calculate = Calculate(api_key=API_KEY, year=year, source="acs", geography=geography)

    dfs = []
    for inputs in acs_variables:
        var = inputs["pff_variable"]
        geo = inputs["geotype"]
        column_mapping = inputs["column_mapping"]
        try:
            dff = calculate(var, geo).rename(columns=column_mapping)[
                ["census_geoid"] + list(column_mapping.values())
            ]
            dfs.append(dff)
            print(f"✅ SUCCESS: {var}\t{geo}", file=sys.stdout)
        except:
            print(f"⛔️ FAILURE: {var}\t{geo}", file=sys.stdout)

    df = reduce(
        lambda left, right: pd.merge(left, right, on=["census_geoid"], how="outer"), dfs
    )
    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output/acs_community_profiles/year={year}/geography={geography}"
    os.makedirs(output_folder, exist_ok=True)
    df.to_csv(f"{output_folder}/acs_community_profiles.csv", index=False)
