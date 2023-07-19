# This script transforms and uploads ACS data provided by the NYCDCP Population team
import argparse
import os
from typing import Tuple

import pandas as pd
import re

OUTPUT_SCHEMA_COLUMNS = [
    "census_geoid",
    "labs_geoid",
    "geotype",
    "labs_geotype",
    "pff_variable",
    "c",
    "e",
    "m",
    "p",
    "z",
    "domain",
]


def parse_args() -> Tuple[str, str]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="The ACS5 year, e.g. 2020 (2016-2020)",
        choices=["2010", "2020", "2021"],
    )
    parser.add_argument(
        "-g",
        "--geography",
        type=str,
        help="The geography year, e.g. 2010_to_2020",
    )
    args = parser.parse_args()
    return args.year, args.geography


def pivot_field_name(df, field_name, domain):
    field_name_df = split_by_field_name(df, field_name)

    field_name_df.rename(
        columns=lambda column_name: re.sub(
            f"^{ field_name }(E|M|C|P|Z)$", r"\1", column_name
        ).lower(),
        inplace=True,
    )
    field_name_df["pff_variable"] = field_name.lower()
    field_name_df["domain"] = domain

    return field_name_df


def extract_field_names(df):
    return df.columns.drop(["GeoType", "GeoID"]).str[:-1].drop_duplicates()


def split_by_field_name(df, pff_field_name):
    return df.filter(regex=f"^(GeoType|GeoID|{ pff_field_name }(E|M|C|P|Z))$")


def strip_unnamed_columns(df):
    return df.loc[:, ~df.columns.str.match("Unnamed")]


def sheet_names(year):
    # NOTE: inflated sheet choices depend on which year's dollars the app should represent
    # Sheet name inflation suffixes are either "_Inflated" or "_NotInflated"
    # e.g. When building for the app in 2022 we want to use inflated 2010 data
    if year == "2010":
        sheet_name_suffix = "0610"
        inflated = "_Inflated"
    elif year == "2020":
        sheet_name_suffix = "1620"
        inflated = ""
    elif year == "2021":
        sheet_name_suffix = "1721"
        inflated = ""
    else:
        raise ValueError("Unknown year '{year}'. Unable to determine sheet name suffix")

    domains_sheets = [
        {"domain": "demographic", "sheet_name": f"Dem{sheet_name_suffix}"},
        {"domain": "social", "sheet_name": f"Social{sheet_name_suffix}"},
        {"domain": "economic", "sheet_name": f"Econ{sheet_name_suffix}{inflated}"},
        {"domain": "housing", "sheet_name": f"Housing{sheet_name_suffix}{inflated}"},
    ]
    return domains_sheets


def transform_dataframe(df, domain):
    df = strip_unnamed_columns(df)
    pff_field_names = extract_field_names(df)
    output_df = pd.DataFrame()

    for field_name in pff_field_names:
        new_df = pivot_field_name(df, field_name, domain)

        if output_df.empty:
            output_df = new_df
        else:
            output_df = pd.concat([output_df, new_df], ignore_index=True)
    return output_df


def transform_all_dataframes(year):
    domains_sheets = sheet_names(year)
    input_file = f"factfinder/data/acs_manual_updates/{year}/acs_{year}.xlsx"

    dfs = pd.read_excel(input_file, sheet_name=None, engine="openpyxl")
    combined_df = pd.DataFrame()

    for domain_sheet in domains_sheets:
        combined_df = pd.concat(
            [
                combined_df,
                transform_dataframe(
                    dfs[domain_sheet["sheet_name"]], domain_sheet["domain"]
                ),
            ]
        )
        print(f"shape of {domain_sheet}: {combined_df.shape}")

    combined_df.dropna(subset=["geotype"], inplace=True)

    return combined_df


def filter_by_metadata(df, year):
    metadata_file = f"factfinder/data/acs/{year}/metadata.json"
    acs_variable_mapping = pd.read_json(metadata_file)[["pff_variable"]]

    return df.merge(acs_variable_mapping, how="inner", on="pff_variable")


def rename_columns(df):
    df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    return df.reindex(columns=OUTPUT_SCHEMA_COLUMNS)


if __name__ == "__main__":
    # Get ACS year
    year, geography = parse_args()

    print("transform_all_dataframes ...")
    export_df = transform_all_dataframes(year)

    print("filter_by_metadata ...")
    export_df = filter_by_metadata(export_df, year)

    print("rename_columns ...")
    export_df = rename_columns(export_df)

    output_folder = f".output/acs/year={year}/geography={geography}"

    print("export_df.to_csv ...")
    os.makedirs(output_folder, exist_ok=True)
    export_df.to_csv(f"{output_folder}/acs_manual_update.csv", index=False)
