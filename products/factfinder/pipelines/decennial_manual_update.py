import pandas as pd
from pathlib import Path
import shutil

from dcpy.utils.json import df_to_json
from dcpy.utils.logging import logger
from dcpy.builds import load
from . import DATA_PATH
from .utils import s3_upload


SHEET_CONFIG = {
    "dcp_pop_decennial_dhc": {
        "2010": "2010 (in 2020 Geogs)",
        "2020": "2020",
        "data_dictionary": "Data Dictionary",
        "geotype_column": "geogtype",
        "skiprows": 3,
    },
    "dcp_pop_decennial_ddhca": {
        "2010": "DDHCA-Equiv_Data_2010",
        "2020": "DDHCA_Data_2020",
        "data_dictionary": "DDHCA_Data Dictionary",
        "geotype_column": "geotype",
    },
}
YEARS = ["2010", "2020"]

PIVOT_COLUMNS = ["year", "geoid"]

COLUMN_CLEANUP = {"Male ": "Male", "Male P": "MaleP"}

OUTPUT_FOLDER = DATA_PATH / ".output" / "decennial"


def clean_data(df: pd.DataFrame, geotype: str) -> pd.DataFrame:
    """Edits geoid column for community districts to avoid collisions with boros"""
    df["geoid"] = df.apply(
        lambda x: "CCD" + x["geoid"] if x[geotype] == "CCD2023" else x["geoid"],
        axis=1,
    )
    df.drop(geotype, axis=1, inplace=True)
    return df


def process_data_sheet(
    excel_file: Path, year: str, sheet_name: str, geotype: str
) -> None:
    """Process single sheet of decennial data from population.
    Cleans column names and pivots wide to long based on geoid"""
    logger.info(f"Processing data for decennial year {year} using sheet '{sheet_name}'")
    output_file = OUTPUT_FOLDER / year / "decennial.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.rename(columns=COLUMN_CLEANUP, inplace=True)
    df.columns = df.columns.str.lower()
    df["geoid"] = df["geoid"].astype(str)

    df = clean_data(df, geotype)
    if "year" not in df.columns:
        df["year"] = year
    df = df.melt(id_vars=PIVOT_COLUMNS)

    df.to_csv(output_file, index=False, mode="a")


def process_metadata(excel_file: Path, sheet_name: str, skiprows: int | None = None):
    """From excel input file, convert Data Dictionary sheet to json files
    Generates one file per year, based on 'Dataset' column which specifies which datasets (2010, 2020, or both)
    the field is present in"""
    df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skiprows)
    df = df.dropna(subset=["Category"])
    df["VariableName"] = df["VariableName"].apply(lambda c: COLUMN_CLEANUP.get(c, c))
    df["year"] = df["Dataset"].astype(str).str.split(", ")
    df = df.explode("year")
    columns = {
        "VariableName": "pff_variable",
        "Relation": "base_variable",
        "Category": "category",
    }
    df.rename(columns=columns, inplace=True)
    files = {}
    for year, year_df in df.groupby("year"):
        year_df = year_df[columns.values()]
        year_df["domain"] = "decennial"
        file = OUTPUT_FOLDER / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        with open(file, "a") as outfile:
            outfile.write(df_to_json(year_df))
        files[year] = file
    return files


def process_file(dataset: str, excel: Path):
    """Process excel file from population
    Assumes that 2010 and 2020 decennial data are both present as well as Data Dictionary sheet
    Generates one pivoted csv output, one metadata.json file per year"""
    for year in YEARS:
        process_data_sheet(
            excel,
            year,
            SHEET_CONFIG[dataset][year],
            SHEET_CONFIG[dataset]["geotype_column"],
        )
    process_metadata(
        excel,
        SHEET_CONFIG[dataset]["data_dictionary"],
        skiprows=SHEET_CONFIG[dataset].get("skiprows"),
    )


def run(load_result: load.LoadResult, upload: bool = False):
    shutil.rmtree(OUTPUT_FOLDER, ignore_errors=True)

    for dataset in load_result.datasets:
        file_path = load.get_imported_filepath(load_result, dataset)
        process_file(dataset, file_path)

    if upload:
        for year in YEARS:
            shutil.copy("build_metadata.json", OUTPUT_FOLDER / year)
            s3_upload(OUTPUT_FOLDER / year)
