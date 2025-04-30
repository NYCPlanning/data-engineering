"""Code to output two indicators,
"Percent of residents within 1/4 mile of ADA accessible subway stations\" and
"Percent within 1/4 mile of subway or Select Bus station". Both indicators are similar and quite simple
"""

import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import puma_to_borough
from ingest.ingestion_helpers import read_from_excel

# TODO: change back to 2025
SOURCE_DATA_FILE = "resources/quality_of_life/EDDE_2024_Updates_transportation.xlsx"
CATEGORY = "quality_of_life"
SOURCE_SHEET_NAMES = {
    "subway_SBS": "Subway_SBS_Qr_Mile_Access",
    "ada_subway": "ADA_Subway_Qtr_Mile_Access",
}
COLUMN_MAPPINGS = {
    "puma": "PUMA",
    "pop_with_access_subway_SBS": "Pop within 1/4 Mile of Subway Stations and SBS Stops",
    "pop_with_accessible_ADA_subway": "Pop within 1/4 Mile of ADA Subway Stations",
    "total_pop": "Total_Pop21",
}


def access_subway_and_access_ADA(geography, save_for_internal_review=False):
    """Accessor for two similar indicators:
    - Percent of residents within 1/4 mile of ADA accessible subway stations
    - Percent within 1/4 mile of subway or Select Bus station"""

    assert geography in ["puma", "borough", "citywide"]
    subway_SBS_ind_name = "access_subwaysbs_pct"
    ADA_ind_name = "access_ada_pct"

    access_subway_SBS = load_access_subway_SBS()
    access_ADA_subway = load_access_ADA_subway()

    assign_geo_cols(access_subway_SBS)
    assign_geo_cols(access_ADA_subway)

    subway_fraction = calculate_access_fraction(
        access_subway_SBS,
        geography,
        COLUMN_MAPPINGS["pop_with_access_subway_SBS"],
        subway_SBS_ind_name,
    )
    ADA_fraction = calculate_access_fraction(
        access_ADA_subway,
        geography,
        COLUMN_MAPPINGS["pop_with_accessible_ADA_subway"],
        ADA_ind_name,
    )
    subway_and_ADA_access = subway_fraction.merge(
        ADA_fraction, left_index=True, right_index=True
    )
    if save_for_internal_review:
        set_results_for_internal_review(
            access_df=subway_and_ADA_access, geography=geography
        )

    return subway_and_ADA_access[[subway_SBS_ind_name, ADA_ind_name]]


def assign_geo_cols(access_dataset):
    access_dataset["borough"] = access_dataset.apply(axis=1, func=puma_to_borough)

    access_dataset["citywide"] = "citywide"


def set_results_for_internal_review(access_df, geography):
    """Saves results to .csv so that reviewers can see results during code review"""
    set_internal_review_files(
        data=[
            (access_df, "Access_to_subway_or_sbs.csv", geography),
        ],
        category="quality_of_life",
    )


def calculate_access_fraction(data, gb_col, count_col, fraction_col):
    data[count_col] = pd.to_numeric(data[count_col])
    data["total_pop"] = pd.to_numeric(data[COLUMN_MAPPINGS["total_pop"]])
    gb = data.groupby(gb_col).sum()
    gb[fraction_col] = ((gb[count_col] / gb["total_pop"]) * 100).round(2)
    return gb[[fraction_col]]


def load_access_subway_SBS() -> pd.DataFrame:
    access = read_from_excel(
        file_path=SOURCE_DATA_FILE,
        category=CATEGORY,
        sheet_name=SOURCE_SHEET_NAMES["subway_SBS"],
    )
    access = remove_state_code_from_PUMA(access)
    return access


def remove_state_code_from_PUMA(access: pd.DataFrame) -> pd.DataFrame:
    access["puma"] = access[COLUMN_MAPPINGS["puma"]].astype(str).str[-5:]
    return access


def load_access_ADA_subway() -> pd.DataFrame:
    access = read_from_excel(
        file_path=SOURCE_DATA_FILE,
        category=CATEGORY,
        sheet_name=SOURCE_SHEET_NAMES["ada_subway"],
    )
    access = remove_state_code_from_PUMA(access)
    return access
