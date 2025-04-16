import json
import sys
from functools import reduce
from pathlib import Path
import pandas as pd
from pathos.pools import ProcessPool

from factfinder.calculate import Calculate

from . import API_KEY, DATA_PATH
from .utils import parse_args

with open(DATA_PATH / "acs_community_profiles" / "metadata.json", "r") as f:
    acs_variables = json.load(f)

if __name__ == "__main__":
    # Get ACS year
    year, geography, upload = parse_args()
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
        except:  # noqa: E722
            print(f"⛔️ FAILURE: {var}\t{geo}", file=sys.stdout)

    df = reduce(
        lambda left, right: pd.merge(left, right, on=["census_geoid"], how="outer"), dfs
    )
    # Concatenate dataframes and export to 1 large csv
    output_file = (
        Path(".output/acs_community_profiles") / year / "acs_community_profiles.csv"
    )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
