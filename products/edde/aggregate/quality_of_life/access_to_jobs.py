import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import clean_PUMAs, puma_to_borough


def access_to_jobs(geography, write_to_internal_review=False):
    indicator_col_name = "access_employment_count"
    clean_df = load_clean_source_data(indicator_col_name)

    final = clean_df.groupby(geography).sum()[[indicator_col_name]]
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "access_employment.csv", geography)],
            "quality_of_life",
        )
    return final


def load_clean_source_data(indicator_col_name) -> pd.DataFrame:
    source_data = pd.read_csv(
        "resources/quality_of_life/access_to_jobs.csv",
        usecols=[
            "PUMA",
            "Weighted Average Number of Jobs Accessible within 30 mins from Tract Centroid by Transit",
        ],
    )
    col_name_mapper = {
        "PUMA": "puma",
        "Weighted Average Number of Jobs Accessible within 30 mins from Tract Centroid by Transit": indicator_col_name,
    }
    source_data.rename(columns=col_name_mapper, inplace=True)
    source_data["puma"] = source_data["puma"].apply(func=clean_PUMAs)
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    source_data["citywide"] = "citywide"
    return source_data
