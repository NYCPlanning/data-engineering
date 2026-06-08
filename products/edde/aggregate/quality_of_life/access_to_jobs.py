import pandas as pd
from resources import load
from utils.geo_helpers import clean_PUMAs, puma_to_borough

INDICATOR_COL_NAME = "access_employment_count"


def access_to_jobs(geography):
    final = load_clean_source_data().groupby(geography).sum()[[INDICATOR_COL_NAME]]
    return final


def load_clean_source_data() -> pd.DataFrame:
    source_data = load("transportation_jobs_access").rename(
        columns={
            "PUMA": "puma",
            "Weighted Average Number of Jobs Accessible within 30 mins from Tract Centroid by Transit": INDICATOR_COL_NAME,
        }
    )

    source_data["puma"] = source_data["puma"].apply(func=clean_PUMAs)
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    source_data["citywide"] = "citywide"
    return source_data
