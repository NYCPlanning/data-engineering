import pandas as pd
from itertools import groupby
import re
from src.digital_ocean_utils import DigitalOceanClient

REPO_NAME = "db-equitable-development-tool"
S3_FOLDER_NAME = "db-eddt"
BUCKET_NAME = "edm-publishing"

demographic_categories = ["demographics", "economics"]

other_categories = [
    "housing_production",
    "housing_security",
    "quality_of_life",
]

geographies = ["citywide", "borough", "puma"]


def get_demographics_data(branch: str, version: str):
    parent_dir = f"https://{BUCKET_NAME}.nyc3.digitaloceanspaces.com/{S3_FOLDER_NAME}/{branch}/{version}"
    data = {}
    client = DigitalOceanClient(BUCKET_NAME, S3_FOLDER_NAME)
    for category in demographic_categories:
        category_data = {}
        files = client.get_all_filenames_in_folder(
            f"db-eddt/{branch}/{version}/{category}"
        )
        matches = [
            re.match(
                "^(demographics|economics)_(\d{4})_(citywide|borough|puma).csv$", file
            )
            for file in files
        ]
        key = lambda m: (m.group(1), m.group(3))
        for (category_match, geography), matches in groupby(
            sorted(matches, key=key), key=key
        ):
            if category_match != category:
                raise Exception(f'Unknown demographics category "{category_match}"')
            category_data[geography] = {}
            for match in matches:
                year = match.group(2)
                category_data[geography][year] = pd.read_csv(
                    f"{parent_dir}/{category}/{category}_{year}_{geography}.csv"
                )

        data[category] = category_data

    return data


def get_other_data(branch: str, version: str):
    parent_dir = f"https://{BUCKET_NAME}.nyc3.digitaloceanspaces.com/{S3_FOLDER_NAME}/{branch}/{version}"
    data = {}
    for category in other_categories:
        category_data = {}
        for geography in geographies:
            category_data[geography] = pd.read_csv(
                f"{parent_dir}/{category}/{category}_{geography}.csv"
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
