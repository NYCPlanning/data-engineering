import re
import pandas as pd
from datetime import datetime
import json
from typing import Dict
from dotenv import load_dotenv
import streamlit as st

from dcpy.utils import s3
from src.digital_ocean_utils import DigitalOceanClient

load_dotenv()

S3_BUCKET_NAME = "edm-publishing"
S3_DATASET_NAME = "db-pluto"


def get_output_folder_path(branch: str) -> str:
    return f"{S3_DATASET_NAME}/{branch}/latest/output"


def get_data(branch: str) -> Dict[str, pd.DataFrame]:
    data = {}
    url = f"https://edm-publishing.nyc3.digitaloceanspaces.com/{get_output_folder_path(branch)}"

    client = DigitalOceanClient(bucket_name=S3_BUCKET_NAME, repo_name=S3_DATASET_NAME)
    kwargs = {"true_values": ["t"], "false_values": ["f"]}

    data["df_mismatch"] = client.csv_from_DO(
        url=f"{url}/qaqc/qaqc_mismatch.csv", kwargs=kwargs
    )
    data["df_null"] = client.csv_from_DO(url=f"{url}/qaqc/qaqc_null.csv", kwargs=kwargs)
    data["df_aggregate"] = client.csv_from_DO(
        url=f"{url}/qaqc/qaqc_aggregate.csv", kwargs=kwargs
    )
    data["df_expected"] = client.csv_from_DO(
        url=f"{url}/qaqc/qaqc_expected.csv",
        kwargs={"converters": {"expected": json.loads}} | kwargs,
    )
    data["df_outlier"] = client.csv_from_DO(
        url=f"{url}/qaqc/qaqc_outlier.csv",
        kwargs={"converters": {"outlier": json.loads}} | kwargs,
    )

    data = data | get_changes(client, branch)

    data["source_data_versions"] = client.csv_from_DO(
        url=f"{url}/source_data_versions.csv"
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


def get_changes(client: DigitalOceanClient, branch: str) -> Dict[str, pd.DataFrame]:
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
    output_filenames = s3.get_filenames(S3_BUCKET_NAME, get_output_folder_path(branch))

    for changes_files_group in valid_changes_files_group:
        if changes_files_group["zip_filename"] in output_filenames:
            pluto_changes_zip = client.zip_from_DO(
                zip_filename=f"db-pluto/{branch}/latest/output/{changes_files_group['zip_filename']}",
            )
            changes["pluto_changes_applied"] = client.unzip_csv(
                csv_filename=changes_files_group["applied_filename"],
                zipfile=pluto_changes_zip,
            )
            changes["pluto_changes_not_applied"] = client.unzip_csv(
                csv_filename=changes_files_group["applied_filename"],
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
    return s3.get_subfolders(S3_BUCKET_NAME, S3_DATASET_NAME)


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
