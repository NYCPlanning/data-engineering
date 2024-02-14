# This script transforms and uploads ACS data provided by the NYCDCP Population team
import pandas as pd
from pathlib import Path
import shutil

from dcpy.utils import json, string, logging
from dcpy.builds import load

from . import OUTPUT_FOLDER
from .utils import export_df, s3_upload, pivot_factfinder_table

DATASET = "acs"
DOMAINS = ["demographic", "economic", "housing", "social"]

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


def process_metadata(excel_file: Path) -> dict[str, Path]:
    df = pd.read_excel(excel_file, sheet_name="ACS Data Dictionary", skiprows=2)
    df = df.dropna(subset=["Category"])
    df["VariableName"] = df["VariableName"].str.lower()
    df["year"] = df["Dataset"].astype(str).str.split(", ")
    df = df.explode("year")
    df["year"] = df["year"].astype(str).str.split("-").apply(lambda x: x[1])
    columns = {column: string.camel_to_snake(column) for column in df.columns}
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
        year = str(year)
        year_df = year_df[columns.values()]
        df.drop(axis=1, labels=["year", "dataset", "new"])
        file = OUTPUT_FOLDER / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        with open(file, "w") as outfile:
            outfile.write(json.df_to_json(year_df))
        files[year] = file
    return files


def sheet_names(year: str) -> dict[str, str]:
    short_year = year[-2:]
    start_year = int(short_year) - 4
    sheet_name_suffix = f"{start_year:02}{short_year:02}"

    domains_sheets = {
        "demographic": f"Dem{sheet_name_suffix}",
        "social": f"Social{sheet_name_suffix}",
        "economic": f"Econ{sheet_name_suffix}",
        "housing": f"Housing{sheet_name_suffix}",
    }
    return domains_sheets


def strip_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.str.match("Unnamed")]


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    return df.reindex(columns=OUTPUT_SCHEMA_COLUMNS)


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_unnamed_columns(df)
    df.rename(columns=str.lower, inplace=True)
    df.dropna(subset=["geotype"], inplace=True)

    df = pivot_factfinder_table(df)
    return df


def process_domain_data(df: pd.DataFrame, domain: str, year: str):
    df = transform_dataframe(df)
    df["domain"] = domain
    df = rename_columns(df)
    export_df(df, DATASET, year, copy_metadata=True)


def process_2010_data(load_result: load.LoadResult):
    for domain in DOMAINS:
        logging.logger.info(f"Processing ACS year 2010 domain {domain}")
        df = load.get_imported_df(load_result, f"dcp_pop_acs2010_{domain}")
        process_domain_data(df, domain, "2010")


def process_latest_data(file: Path, year: str):
    domain_sheets = sheet_names(year)

    for domain in DOMAINS:
        logging.logger.info(f"Processing ACS year {year} domain {domain}")
        df = pd.read_excel(file, sheet_name=domain_sheets[domain])
        process_domain_data(df, domain, year)


def run(load_result: load.LoadResult, upload: bool = False):
    if OUTPUT_FOLDER.is_dir():
        shutil.rmtree(OUTPUT_FOLDER)
    process_2010_data(load_result)

    latest = load_result.datasets["dcp_pop_acs"]
    assert isinstance(latest.destination, Path)
    process_latest_data(latest.destination, latest.version)

    if upload:
        s3_upload(DATASET, ["2010", latest.version])
