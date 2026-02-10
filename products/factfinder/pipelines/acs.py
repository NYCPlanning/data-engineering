import sys
from pathlib import Path

import pandas as pd
from pathos.pools import ProcessPool

from factfinder.calculate import Calculate

from . import API_KEY
from .utils import parse_args


def _calculate(args):
    var, domain, geo, calculate = args
    try:
        df = calculate(var, geo).assign(domain=domain)
        print(f"✅ SUCCESS: {var}\t{geo}", file=sys.stdout)
        return df
    except:  # noqa: E722
        print(f"⛔️ FAILURE: {var}\t{geo}", file=sys.stdout)


if __name__ == "__main__":
    # Get ACS year
    year, geography, _upload = parse_args()
    pool = ProcessPool(nodes=10)

    # Initialize pff instance
    calculate = Calculate(api_key=API_KEY, year=year, source="acs", geography=geography)

    # Declare geography and variables involved in this caculation
    geogs = ["NTA", "CDTA", "CT20", "city", "borough"]
    if geography != "2010_to_2020":
        geogs.extend(["tract"])
    domains = ["demographic", "economic", "housing", "social"]
    variables = [
        (i["pff_variable"], i["domain"], j, calculate)
        for i in calculate.meta.metadata
        for j in geogs
        if i["domain"] in domains
    ]

    # Loop through calculations and collect dataframes in dfs
    dfs = pool.map(_calculate, variables)

    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output/acs/year={year}/geography={geography}"
    df = pd.concat(dfs)

    output_file = Path(".output/acs") / year / "acs.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
