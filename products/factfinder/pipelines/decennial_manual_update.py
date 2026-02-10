import shutil
from pathlib import Path

import pandas as pd

from dcpy.lifecycle.builds import load
from dcpy.utils.logging import logger

from . import OUTPUT_FOLDER
from .utils import apply_ccd_prefix, export_df, process_metadata

DATASET = "decennial"
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


def process_data_sheet(
    excel_file: Path, year: str, sheet_name: str, geotype: str
) -> None:
    """Process single sheet of decennial data from population.
    Cleans column names and pivots wide to long based on geoid"""
    logger.info(f"Processing data for decennial year {year} using sheet '{sheet_name}'")
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    df.rename(columns=COLUMN_CLEANUP, inplace=True)
    df.columns = df.columns.str.lower()
    df["geoid"] = df["geoid"].astype(str)

    df = apply_ccd_prefix(df, geotype=geotype)
    df.drop(geotype, axis=1, inplace=True)
    if "year" not in df.columns:
        df["year"] = year
    df = df.melt(id_vars=PIVOT_COLUMNS)
    return df


def process_file(dataset: str, excel: Path):
    """Process excel file from population
    Assumes that 2010 and 2020 decennial data are both present as well as Data Dictionary sheet
    Generates one pivoted csv output, one metadata.json file per year"""
    process_metadata(
        DATASET,
        excel,
        SHEET_CONFIG[dataset]["data_dictionary"],
        skiprows=SHEET_CONFIG[dataset].get("skiprows"),
        append=True,
    )
    for year in YEARS:
        df = process_data_sheet(
            excel,
            year,
            SHEET_CONFIG[dataset][year],
            SHEET_CONFIG[dataset]["geotype_column"],
        )
        export_df(df, DATASET, year)


def build(version, load_result: load.LoadResult):
    output_folder = OUTPUT_FOLDER / version / "decennial"
    shutil.rmtree(output_folder, ignore_errors=True)

    for dataset in load_result.datasets:
        file_path = load.get_imported_filepath(load_result, dataset)
        process_file(dataset, file_path)
