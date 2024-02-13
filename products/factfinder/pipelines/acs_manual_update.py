# This script transforms and uploads ACS data provided by the NYCDCP Population team
import pandas as pd
from pathlib import Path
import re
import shutil

from dcpy.utils import json, string, logging
from dcpy.builds import load

from . import DATA_PATH
from .utils import s3_upload

OUTPUT_FOLDER = DATA_PATH / ".output" / "acs"
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


def pivot_field_name(df, field_name, domain):
    field_name_df = split_by_field_name(df, field_name)

    field_name_df.rename(
        columns=lambda column_name: re.sub(
            f"^{ field_name }(e|m|c|p|z)$", r"\1", column_name
        ),
        inplace=True,
    )
    field_name_df["pff_variable"] = field_name
    field_name_df["domain"] = domain

    return field_name_df


def extract_field_names(df: pd.DataFrame) -> set[str]:
    return set(
        [
            column[:-1]
            for column in df.columns.drop(["geotype", "geoid"])
            if not column.endswith(".1")
        ]
    )


def split_by_field_name(df: pd.DataFrame, pff_field_name: str):
    return df.filter(regex=f"^(geotype|geoid|{ pff_field_name }(e|m|c|p|z))$").copy()


def strip_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.str.match("Unnamed")]


def sheet_names(year: str) -> dict[str, str]:
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
        year_df = year_df[columns.values()]
        df.drop(axis=1, labels=["year", "dataset", "new"])
        file = OUTPUT_FOLDER / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        with open(file, "w") as outfile:
            outfile.write(json.df_to_json(year_df))
        files[year] = file
    return files


def transform_dataframe(df: pd.DataFrame, domain: str) -> pd.DataFrame:
    df = strip_unnamed_columns(df)
    df.rename(columns=str.lower, inplace=True)
    df.dropna(subset=["geotype"], inplace=True)

    pff_field_names = extract_field_names(df)

    output_df = pd.DataFrame()

    for field_name in pff_field_names:
        new_df = pivot_field_name(df, field_name, domain)

        if output_df.empty:
            output_df = new_df
        else:
            output_df = pd.concat([output_df, new_df], ignore_index=True)
    output_df = rename_columns(output_df)
    return output_df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    return df.reindex(columns=OUTPUT_SCHEMA_COLUMNS)


def export_year(df: pd.DataFrame, year: str):
    output_file = OUTPUT_FOLDER / year / "acs.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, mode="a", header=not output_file.is_file())
    shutil.copy(DATA_PATH / "acs" / year / "metadata.json", OUTPUT_FOLDER / year)


def process_2010(load_result: load.LoadResult) -> pd.DataFrame:
    for domain in DOMAINS:
        logging.logger.info(f"Processing ACS year 2010 domain {domain}")
        df = load.get_imported_df(load_result, f"dcp_pop_acs2010_{domain}")
        df = transform_dataframe(df, domain)
        export_year(df, "2010")


def process_latest(file: Path, year: str):
    domains_sheets = sheet_names(year)

    for domain_sheet in domains_sheets:
        logging.logger.info(
            f"Processing ACS year {year} domain {domain_sheet['domain']}"
        )
        df = pd.read_excel(file, sheet_name=domain_sheet["sheet_name"])
        df = transform_dataframe(df, domain_sheet["domain"])
        export_year(df, year)


def run(load_result: load.LoadResult, upload: bool = False):
    if OUTPUT_FOLDER.is_dir():
        shutil.rmtree(OUTPUT_FOLDER)
    process_2010(load_result)

    latest = load_result.datasets["dcp_pop_acs"]
    assert isinstance(latest.destination, Path)
    process_latest(latest.destination, latest.version)

    if upload:
        for year in ["2010", latest.version]:
            shutil.copy("build_metadata.json", OUTPUT_FOLDER / year)
            s3_upload(OUTPUT_FOLDER / year)
