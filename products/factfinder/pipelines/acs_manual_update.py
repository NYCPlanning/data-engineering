# This script transforms and uploads ACS data provided by the NYCDCP Population team
import pandas as pd
from pathlib import Path
import shutil

from dcpy.utils import logging
from dcpy.lifecycle.builds import load

from . import OUTPUT_FOLDER, DATA_PATH
from .utils import (
    process_metadata,
    apply_ccd_prefix,
    pivot_factfinder_table,
    export_df,
    s3_upload,
)

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
    df2 = df.copy()
    return df2.loc[:, ~df.columns.str.match("Unnamed")]


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    return df.reindex(columns=OUTPUT_SCHEMA_COLUMNS)


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = strip_unnamed_columns(df)
    df.rename(columns=str.lower, inplace=True)
    df.dropna(subset=["geotype"], inplace=True)
    df = apply_ccd_prefix(df)

    df = pivot_factfinder_table(df)
    for column in ["c", "e", "m", "p", "z"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
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
        df = df[[c for c in df.columns if not c.endswith(".1")]]
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


if __name__ == "__main__":
    process_metadata(
        DATASET,
        Path("Your path here"),
        "ACS Data Dictionary",
        output_folder=DATA_PATH,
        skiprows=2,
    )
