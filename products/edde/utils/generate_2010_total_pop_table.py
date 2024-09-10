"""Eventually this could be rewritten to read from total_pop_by_CD but for now it
uses non_fatal_assault_hospitalizations.csv"""

import pandas as pd

from utils.CD_helpers import add_CD_code


def generate_pop_by_CD():
    assaults = pd.read_csv(
        "resources/quality_of_life/non_fatal_assault_hospitalizations.csv"
    )
    add_CD_code(assaults)
    assaults.rename(columns={"2010 population": "2010_pop"}, inplace=True)
    assaults["2010_pop"] = assaults["2010_pop"].str.replace(",", "").astype(int)
    assaults[["CD_code", "2010_pop"]].to_csv(
        "resources/quality_of_life/2010_pop_by_CD.csv", index=False
    )


if __name__ == "__main__":
    generate_pop_by_CD()
