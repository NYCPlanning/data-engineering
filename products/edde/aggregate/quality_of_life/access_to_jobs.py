import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.geo_helpers import clean_PUMAs, puma_to_borough

SOURCE_DATA_FILE = "resources/quality_of_life/EDDE_2025_Updates_transportation.xlsx"
INDICATOR_COL_NAME = "access_employment_count"


def access_to_jobs(geography, write_to_internal_review=False):
    final = load_clean_source_data().groupby(geography).sum()[[INDICATOR_COL_NAME]]
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "access_employment.csv", geography)],
            "quality_of_life",
        )
    return final


def load_clean_source_data() -> pd.DataFrame:
    source_data = pd.read_excel(
        SOURCE_DATA_FILE,
        sheet_name="Access_to_Jobs",
        dtype={"PUMA": str},
    ).rename(
        columns={
            "PUMA": "puma",
            "Weighted Average Number of Jobs Accessible within 30 mins from Tract Centroid by Transit": INDICATOR_COL_NAME,
        }
    )

    source_data["puma"] = source_data["puma"].apply(func=clean_PUMAs)
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    source_data["citywide"] = "citywide"
    return source_data
