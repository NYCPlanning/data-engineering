import argparse
from datetime import date
import json
import pandas as pd
from pathlib import Path
import re
import shutil
from typing import Tuple

from dcpy.connectors.edm import publishing
from dcpy.lifecycle.builds import metadata
from dcpy.utils import string
from . import DATA_PATH, OUTPUT_FOLDER
from ..paths import ROOT_PATH


COLUMN_CLEANUP = {"Male ": "Male", "Male P": "MaleP"}


def process_metadata(
    dataset: str,
    excel_file: Path,
    sheet_name: str,
    skiprows: int = 0,
    output_folder: Path = OUTPUT_FOLDER,
    append=False,
) -> dict[str, Path]:
    df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skiprows)
    df = df.dropna(subset=["Category"])
    df["year"] = df["Dataset"].astype(str).str.split(", ")
    df = df.explode("year")
    df["year"] = (
        df["year"].astype(str).apply(lambda x: x.split("-")[1] if "-" in x else x)
    )
    columns = {column: string.camel_to_snake(column) for column in df.columns}
    columns.update(
        {
            "VariableName": "pff_variable",
            "Relation": "base_variable",
            "Profile": "domain",
        }
    )
    df.rename(columns=columns, inplace=True)
    df["pff_variable"] = df["pff_variable"].apply(lambda c: COLUMN_CLEANUP.get(c, c))
    df["pff_variable"] = df["pff_variable"].str.lower()
    df["base_variable"] = df["base_variable"].str.lower()
    df["domain"] = df["domain"].str.lower()
    df["order"] = df["order"].astype(int)

    string_dtypes = df.convert_dtypes().select_dtypes("string")
    df[string_dtypes.columns] = string_dtypes.apply(lambda x: x.str.strip())

    files: dict[str, Path] = {}
    for year, year_df in df.groupby("year"):
        year = str(year)
        year_df.drop(
            columns=["year", "dataset", "new", "notin_profile"],
            errors="ignore",
            inplace=True,
        )
        year_dict = year_df.to_dict(orient="records")

        file = output_folder / dataset / year / "metadata.json"
        file.parent.mkdir(parents=True, exist_ok=True)
        if append and file.exists():
            with open(file, "r") as f:
                old_data = json.load(f)
            year_dict = old_data + year_dict
        with open(file, "w") as f:
            json.dump(year_dict, f, indent=4)
        files[year] = file

    return files


def apply_ccd_prefix(
    df: pd.DataFrame, geoid: str = "geoid", geotype: str = "geotype"
) -> pd.DataFrame:
    """Edits geoid column for city council districts to avoid collisions with boros"""
    df[geoid] = df.apply(
        lambda x: (
            "CCD" + str(int(x[geoid])) if x[geotype] == "CCD2023" else str(x[geoid])
        ),
        axis=1,
    )
    return df


def regex_or(cases: list[str]) -> str:
    return "|".join([re.escape(s) for s in cases])


def pivot_field_name(
    df: pd.DataFrame,
    field_name: str,
    suffixes: list[str],
    variable_column: str = "variable",
):
    df = df.rename(
        columns=lambda column_name: re.sub(
            f"^{re.escape(field_name)}({regex_or(suffixes)})$", r"\1", column_name
        ),
        errors="raise",
    )
    df[variable_column] = field_name
    return df


def pivot_table_with_suffixes(
    df: pd.DataFrame,
    pivot_columns: list[str],
    suffixes: list[str],
    *,
    only_variables_with_all_suffixes: bool = True,
    variable_column: str = "variable",
) -> pd.DataFrame:
    incompatible_suffixes: list[tuple[str, str]] = []
    for i, suffix in enumerate(suffixes):
        for j, _suffix in enumerate(suffixes):
            if i != j and (suffix.endswith(_suffix) or _suffix.endswith(suffix)):
                incompatible_suffixes.append((suffix, _suffix))
    if len(incompatible_suffixes) > 0:
        raise Exception(f"Non-unique suffixes provided: {incompatible_suffixes}")

    suffix_regex = regex_or(suffixes)
    field_names = {
        re.sub(f"^(.*)({suffix_regex})$", r"\1", column)
        for column in df.columns.drop(pivot_columns)
        if re.match(f"^(.*)({suffix_regex})$", column)
    }

    suffix_columns = []
    field_names_missing_suffixes = set()
    for field_name in field_names:
        for suffix in suffixes:
            column = field_name + suffix
            suffix_columns.append(column)
            if column not in df.columns:
                print(f"{field_name}: {suffix}")
                df[column] = None
                field_names_missing_suffixes.add(field_name)
    if only_variables_with_all_suffixes and len(field_names_missing_suffixes) > 0:
        raise Exception(f"Some field names missing certain suffixes: {field_names}")

    expected_columns = set(pivot_columns + suffix_columns)
    for c in df.columns:
        if c not in expected_columns:
            print(f"unexpected: {c}")
    for c in expected_columns:
        if c not in df.columns:
            print(f"missing: {c}")
    assert set(df.columns) == expected_columns

    output_df = pd.DataFrame()

    for field_name in field_names:
        suffix_columns = [field_name + s for s in suffixes]
        columns = pivot_columns + suffix_columns
        field_df = df[columns].copy()
        field_df = pivot_field_name(
            field_df, field_name, suffixes, variable_column=variable_column
        )
        df.drop(columns=suffix_columns, inplace=True)
        if output_df.empty:
            output_df = field_df
        else:
            output_df = pd.concat([output_df, field_df], ignore_index=True)

    output_df = output_df[pivot_columns + [variable_column] + suffixes]
    return output_df


def pivot_factfinder_table(df: pd.DataFrame) -> pd.DataFrame:
    return pivot_table_with_suffixes(
        df,
        ["geotype", "geoid"],
        ["c", "e", "m", "p", "z"],
        variable_column="pff_variable",
    )


def export_df(df: pd.DataFrame, dataset: str, year: str, copy_metadata=False):
    output_file = OUTPUT_FOLDER / dataset / year / f"{dataset}.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    header = not output_file.exists()
    df.to_csv(output_file, index=False, mode="a", header=header)
    if copy_metadata:
        shutil.copy(
            DATA_PATH / dataset / year / "metadata.json", OUTPUT_FOLDER / dataset / year
        )


def s3_upload(dataset: str, years: list[str], latest=True):
    for year in years:
        output = OUTPUT_FOLDER / dataset / year
        shutil.copy(str(ROOT_PATH / "build_metadata.json"), output)
        publishing.legacy_upload(
            output=output,
            publishing_folder="db-factfinder",
            version=str(date.today()),
            acl="public-read",
            s3_subpath=str(Path(metadata.build_name()) / dataset / year),
            latest=latest,
            contents_only=True,
        )


def melt_wide_df(ds_id: str, df: pd.DataFrame):
    return df.melt(id_vars=list(df.columns[0:2]), value_vars=list(df.columns[2:]))


def parse_args() -> Tuple[str, str, bool]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y",
        "--year",
        type=str,
        help="The ACS5 year, e.g. 2020 (2016-2020)",
    )
    parser.add_argument(
        "-g",
        "--geography",
        type=str,
        help="The geography year, e.g. 2010_to_2020",
    )
    parser.add_argument(
        "-u",
        "--upload",
        action="store_const",
        const=True,
        default=False,
        help="Uploads to Digital Ocean",
    )

    args = parser.parse_args()
    return args.year, args.geography, args.upload
