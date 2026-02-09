import re
from itertools import groupby

import pandas as pd
import streamlit as st

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.connectors.edm import publishing
from dcpy.utils import s3
from dcpy.utils.git import github

REPO_NAME = "db-equitable-development-tool"
PRODUCT = "db-eddt"
assert PUBLISHING_BUCKET, "PUBLISHING_BUCKET must be defined"
BUCKET = PUBLISHING_BUCKET

demographic_categories = ["demographics", "economics"]

other_categories = [
    "housing_production",
    "housing_security",
    "quality_of_life",
]

geographies = ["citywide", "borough", "puma"]


## phased out for other data products, eventually should use edm.publishing instead of s3
@st.cache_data
def get_active_s3_folders(repo: str, s3_folder: str | None = None):
    default_branch = github.get_default_branch(repo=repo)
    all_branches = github.get_branches(repo=repo, branches_blacklist=[])
    all_folders = s3.get_subfolders(BUCKET, (s3_folder or repo))
    folders = sorted(list(set(all_folders).intersection(set(all_branches))))
    folders.remove(default_branch)
    folders = [default_branch] + folders
    return folders


## TODO - this needs cleaning ideally to get rid of s3 reference, but EDDE needs some rework in versioning, data export
def get_demographics_data(branch: str, version: str):
    data = {}
    for category in demographic_categories:
        category_data: dict[str, dict[str, pd.DataFrame]] = {}
        files = s3.get_filenames(BUCKET, f"{PRODUCT}/{branch}/{version}/{category}")
        pattern = r"^(demographics|economics)_(\d{4})_(citywide|borough|puma).csv$"
        match_objs = [re.match(pattern, file) for file in files]
        matches = [match for match in match_objs if match]

        def key(m):
            return (m.group(1), m.group(3))

        for (category_match, geography), grouped_matches in groupby(
            sorted(matches, key=key), key=key
        ):
            if category_match != category:
                raise Exception(f'Unknown demographics category "{category_match}"')
            category_data[geography] = {}
            for match in grouped_matches:
                year = match.group(2)
                category_data[geography][year] = publishing.read_csv_legacy(
                    PRODUCT,
                    f"{branch}/{version}",
                    f"{category}/{category}_{year}_{geography}.csv",
                )

        data[category] = category_data

    return data


def get_other_data(branch: str, version: str):
    data = {}
    for category in other_categories:
        category_data = {}
        for geography in geographies:
            category_data[geography] = publishing.read_csv_legacy(
                PRODUCT,
                f"{branch}/{version}",
                f"{category}/{category}_{geography}.csv",
            )
        data[category] = category_data
    return data


## essentially, check if string are equal other than change of numeric characters
def compare_header(left: str, right: str):
    if len(left) != len(right):
        return False
    for char1, char2 in zip(left, right):
        if char1 != char2 and not (char1.isdigit() and char2.isdigit()):
            return False
    return True


# desired output - columns in left not in right, columns in right not in left
def compare_columns(left: pd.DataFrame, right: pd.DataFrame, try_pairing: bool):
    dropped = [column for column in left.columns if column not in right.columns]
    added = [column for column in right.columns if column not in left.columns]
    union = [column for column in left.columns if column in right.columns]
    if not try_pairing:
        return dropped, added, union, []  ## todo this is not the most elegant
    else:
        paired_columns = []
        dropped_unpaired = []
        added_paired = set()
        for col in dropped:
            matches = [col2 for col2 in added if compare_header(col, col2)]
            if len(matches) > 0:
                paired_columns.append((col, matches))
                for match in matches:
                    added_paired.add(match)
            else:
                dropped_unpaired.append(col)
        added_unpaired = [col for col in added if col not in added_paired]
        return dropped_unpaired, added_unpaired, union, paired_columns
