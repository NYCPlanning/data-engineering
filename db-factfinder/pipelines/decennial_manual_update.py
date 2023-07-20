# This script transforms and uploads ACS data provided by the NYCDCP Population team
import argparse
import os

import pandas as pd
"""
year	geoid	variable	value
2010	0	ingrpqtrsp	2.3
2010	2	ingrpqtrsp	3.4
"""
geogs = {
    "2010": "2010_to_2020",
    "2020": "2020"
}

sheetnames = {
    "2010": "2010 (in 2020 Geogs)",
    "2020": "2020"
}

column_mapping = {
    "popacre": "popperacre",
    "popacrep": "popperacrep",
    "popinhh_1": "popinhh",
    "popinhh_1p": "popinhhp",
    "popu18": "popu18_1",
    "popu18p": "popu18_1p"
}

pivot_columns = ["year", "geoid"]

def parse_args() -> str:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="The decennial census year, e.g. 2020",
        choices=["2010", "2020"],
    )
    args = parser.parse_args()
    return args.year

def read_excel(year:str) -> pd.DataFrame:
    input_file = f"factfinder/data/decennial_manual_updates/dcp_pop_census_dhc.xlsx"
    df = pd.read_excel(input_file, sheet_name=sheetnames[year], engine="openpyxl")
    df.columns = df.columns.str.lower()
    df["geoid"] = df["geoid"].astype(str)
    print(df.columns)
    return df

def clean_data(df: pd.DataFrame, year: str) -> pd.DataFrame:
    #clean community districts
    df["geoid"] = df.apply(lambda x: "CCD" + x["geoid"] if x["geogtype"]=="CCD2023" else x["geoid"], axis=1)

    #filter columns by what's needed for output
    metadata_file = f"factfinder/data/decennial/{year}/metadata.json"
    output_columns = list(pd.read_json(metadata_file)["pff_variable"])
    output_columns = output_columns + [ column + "p" for column in output_columns ]
    df.columns = [ column_mapping.get(column, column) for column in df.columns ]
    df = df[[ column for column in df.columns if column in (output_columns + pivot_columns) ]]
    return df

def pivot_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.melt(id_vars=pivot_columns)
    return df

if __name__ == "__main__":
    year = parse_args()
    geography = geogs[year]

    print("read in excel ...")
    df = read_excel(year)
    output_folder = f".output/decennial/year={year}/geography={geography}"
    pd.DataFrame({"geogtype": df['geogtype'].unique()}).to_csv(f"{output_folder}/geogtypes.csv", index=False)
    pd.DataFrame({"ColumnName": df.columns}).to_csv(f"{output_folder}/columns.csv", index=False)

    print("clean and pivot df ...")
    df = clean_data(df, year)
    df = pivot_dataframe(df)


    print("export df to_csv ...")
    os.makedirs(output_folder, exist_ok=True)
    df.to_csv(f"{output_folder}/decennial_manual_update.csv", index=False)
