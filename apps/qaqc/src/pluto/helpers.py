import re
import pandas as pd
from datetime import datetime
import json
from typing import Dict
from dotenv import load_dotenv
import streamlit as st

from dcpy.utils import s3
from dcpy.connectors.edm import publishing
from src.constants import BUCKET_NAME
from src import digital_ocean_utils

load_dotenv()

DATASET = "db-pluto"


def get_data(branch: str) -> Dict[str, pd.DataFrame]:
    data = {}
    version = f"{branch}/latest/output"

    def read_csv(qaqc_type, **kwargs):
        return publishing.read_csv(
            DATASET,
            version,
            f"qaqc/qaqc_{qaqc_type}.csv",
            true_values=["t"],
            false_values=["f"],
            **kwargs,
        )

    data["df_mismatch"] = read_csv("mismatch")
    data["df_null"] = read_csv("null")
    data["df_aggregate"] = read_csv("aggregate")
    data["df_expected"] = read_csv("expected", converters={"expected": json.loads})
    data["df_outlier"] = read_csv("outlier", converters={"outlier": json.loads})

    data = data | get_changes(branch)

    data["source_data_versions"] = publishing.read_csv(
        DATASET, version, "source_data_versions.csv"
    )

    data["version_text"] = data["source_data_versions"]

    # standarzie minor versions strings to be dot notation
    # NOTE this is a temporary approach until data-library is improved to use dot notation
    data_to_standardize = ["df_mismatch", "df_null"]
    for data_name in data_to_standardize:
        data[data_name].replace(
            to_replace={
                "23v1_1 - 23v1": "23v1.1 - 23v1",
                "23v1 - 22v3_1": "23v1 - 22v3.1",
            },
            inplace=True,
        )

    return data


def get_changes(branch: str) -> Dict[str, pd.DataFrame]:
    changes = {}
    valid_changes_files_group = [
        # latest set of filenames
        {
            "zip_filename": "pluto_changes.zip",
            "applied_filename": "pluto_changes_applied.csv",
            "not_applied_filename": "pluto_changes_not_applied.csv",
        },
        # a legacy set of filenames
        {
            "zip_filename": "pluto_corrections.zip",
            "applied_filename": "pluto_corrections_applied.csv",
            "not_applied_filename": "pluto_corrections_not_applied.csv",
        },
    ]
    output_filenames = s3.get_filenames(
        BUCKET_NAME, f"{DATASET}/{branch}/latest/output"
    )

    for changes_files_group in valid_changes_files_group:
        if changes_files_group["zip_filename"] in output_filenames:
            pluto_changes_zip = publishing.get_zip(
                DATASET, f"{branch}/latest/output", changes_files_group["zip_filename"]
            )
            changes["pluto_changes_applied"] = digital_ocean_utils.unzip_csv(
                csv_filename=changes_files_group["applied_filename"],
                zipfile=pluto_changes_zip,
            )
            changes["pluto_changes_not_applied"] = digital_ocean_utils.unzip_csv(
                csv_filename=changes_files_group["not_applied_filename"],
                zipfile=pluto_changes_zip,
            )

            return changes

    raise FileNotFoundError(
        f"""
        No valid pluto changes zip file found!
        Files in branch folder "{branch}"
        {output_filenames}
        """
    )


@st.cache_data(ttl=600)
def get_all_s3_folders():
    return s3.get_subfolders(BUCKET_NAME, DATASET)


def get_s3_folders():
    branches = sorted(blacklist_branches(get_all_s3_folders()))
    return branches


def get_past_versions():
    folders = get_all_s3_folders()
    pattern = "^\d{2}v\d(\.\d)?$"  # matches pluto version format of 23v1, 23v1.2, etc
    versions = [f for f in folders if re.match(pattern, f)]
    return sorted(versions, reverse=True)


def convert(dt):
    try:
        d = datetime.strptime(dt, "%Y/%m/%d")
        return d.strftime("%m/%d/%y")
    except BaseException:
        return dt


def blacklist_branches(branches):
    """For pluto this is done by programmatically, can also be hard-coded"""
    valid_branches = []
    date_regex = r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
    for b in branches:
        if (re.match(date_regex, b) is None) and b != "latest":
            valid_branches.append(b)
    return valid_branches
