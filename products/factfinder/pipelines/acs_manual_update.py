# This script transforms and uploads ACS data provided by the NYCDCP Population team
import pandas as pd
import re
import shutil

from dcpy.utils.json import df_to_json
from dcpy.utils.string import camel_to_snake
from dcpy.connectors.edm import recipes
from .utils import parse_args, download_manual_update, s3_upload, DATA_PATH

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

PIVOT_COLUMNS = ["year", "geoid"]

OUTPUT_FOLDER = DATA_PATH / ".output" / "acs"

DOMAINS = ["demographic", "economic", "housing", "social"]
ACS2010_DATASET_NAMES = {s: f"dcp_pop_acs2010_{s}" for s in DOMAINS}


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
    return df.filter(regex=f"^(GeoType|GeoID|{ pff_field_name }(E|M|C|P|Z))$").copy()


def strip_unnamed_columns(df):
    return df.loc[:, ~df.columns.str.match("Unnamed")]


def sheet_names(year):
    short_year = year[-2:]
    start_year = int(short_year) - 4
    sheet_name_suffix = f"{start_year:02}{short_year:02}"

    domains_sheets = [
        {"domain": "demographic", "sheet_name": f"Dem{sheet_name_suffix}"},
        {"domain": "social", "sheet_name": f"Social{sheet_name_suffix}"},
        {"domain": "economic", "sheet_name": f"Econ{sheet_name_suffix}"},
        {"domain": "housing", "sheet_name": f"Housing{sheet_name_suffix}"},
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

    input_file = download_manual_update("acs", year)

    dfs = pd.read_excel(input_file, sheet_name=None, engine="openpyxl")
    combined_df = pd.DataFrame()

    for domain_sheet in domains_sheets:
        df = dfs[domain_sheet["sheet_name"]]
        print(domain_sheet)
        print(df.head())
        transformed = transform_dataframe(df, domain_sheet["domain"])
        combined_df = pd.concat(
            [
                combined_df,
                transformed,
            ]
        )
        print(f"shape of {domain_sheet}: {combined_df.shape}")

    combined_df.dropna(subset=["geotype"], inplace=True)

    return combined_df


def ingest_2010_data():
    dfs = []
    for domain in ACS2010_DATASET_NAMES:
        dataset = recipes.Dataset(name=ACS2010_DATASET_NAMES[domain], version="latest")
        df = recipes.read_df(dataset)
        df = transform_dataframe(df, domain)
        print(f"shape of {domain}: {df.shape}")
        dfs.append(df)

    df = pd.concat(dfs)
    df.dropna(subset=["geotype"], inplace=True)
    return df


def process_metadata(excel_file):
    df = pd.read_excel(excel_file, sheet_name="ACS Data Dictionary", skiprows=2)
    df = df.dropna(subset=["Category"])
    df["VariableName"] = df["VariableName"].str.lower()
    df["year"] = df["Dataset"].astype(str).str.split(", ")
    df = df.explode("year")
    df["year"] = df["year"].astype(str).str.split("-").apply(lambda x: x[1])
    columns = {column: camel_to_snake(column) for column in df.columns}
    columns.update(
        {
            "VariableName": "pff_variable",
            "Relation": "base_variable",
            "Profile": "domain",
        }
    )
    df.rename(columns=columns, inplace=True)
    files = {}
    for year, year_df in df.groupby("year"):
        year_df = year_df[columns.values()]
        df.drop(axis=1, labels=["year", "dataset", "new"])
        file = OUTPUT_FOLDER / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        with open(file, "w") as outfile:
            outfile.write(df_to_json(year_df))
        files[year] = file
    print(files)
    return files


def rename_columns(df):
    df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    return df.reindex(columns=OUTPUT_SCHEMA_COLUMNS)


if __name__ == "__main__":
    # Get ACS year
    year, _geography, upload = parse_args()

    print("transform_all_dataframes ...")
    if year == "2010":
        export_df = ingest_2010_data()
    else:
        export_df = transform_all_dataframes(year)

    print("rename_columns ...")
    export_df = rename_columns(export_df)

    output_file = OUTPUT_FOLDER / year / "acs.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    export_df.to_csv(output_file, index=False)
    shutil.copy(DATA_PATH / "acs" / year / "metadata.json", OUTPUT_FOLDER / year)

    if upload:
        s3_upload(output_file)
