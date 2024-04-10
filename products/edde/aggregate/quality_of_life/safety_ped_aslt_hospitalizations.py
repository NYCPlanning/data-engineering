import numpy as np
import pandas as pd
from utils.CD_helpers import add_CD_code, community_district_to_PUMA
from internal_review.set_internal_review_file import set_internal_review_files


def assault_hospitalizations(geography, write_to_internal_review=False):
    source_data = load_clean_source_data("assaults")
    indicator_col_label = "safety_assaulthospital_rate"
    final = calculate_per100k_rate(source_data, geography)
    final.name = indicator_col_label
    final = pd.DataFrame(final)
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "assault_hospitalizations.csv", geography)],
            category="quality_of_life",
        )
    return final


def pedestrian_hospitalizations(geography, write_to_internal_review=False):
    source_data = load_clean_source_data("pedestrian")
    indicator_col_label = "safety_pedhospital_rate"

    final = calculate_per100k_rate(source_data, geography)
    final.name = indicator_col_label
    final = pd.DataFrame(final)
    if write_to_internal_review:
        set_internal_review_files(
            [(final, "pedestrian_hospitalizations.csv", geography)],
            category="quality_of_life",
        )

    return final


def calculate_per100k_rate(source_data, geography):
    assert geography in ["citywide", "borough", "puma"]
    gb = source_data.groupby(geography).sum()[["Number", "2010_pop"]]
    final = ((gb["Number"] / gb["2010_pop"]) * 10**5).round(2)
    final.replace({0: None}, inplace=True)
    return final


def load_clean_source_data(indicator):
    assert indicator in ["pedestrian", "assaults"]
    read_csv_args = {
        "pedestrian": {
            "filepath_or_buffer": "resources/quality_of_life/pedestrian_injuries.csv",
            "skiprows": 6,
            "nrows": 65,
        },
        "assaults": {
            "filepath_or_buffer": "resources/quality_of_life/non_fatal_assault_hospitalizations.csv"
        },
    }
    source_data = pd.read_csv(**read_csv_args[indicator])

    source_data["GeoTypeName"] = source_data["GeoTypeName"].str.lower()
    source_data["citywide"] = "citywide"
    if source_data["Number"].dtype == np.dtype("object"):
        source_data["Number"] = (
            source_data["Number"].str.replace(",", "").astype(float, errors="raise")
        )
    add_CD_code(source_data)
    if "2010 Population" in source_data.columns:
        source_data.rename({"2010 Population": "2010_pop"}, inplace=True)
    else:
        source_data = add_2010_population(source_data)

    source_data = community_district_to_PUMA(source_data, "CD_code")
    return source_data


def add_2010_population(df):
    """Add column of 2010 population from assault non-fatal hospitalizations numbers"""

    pop_2010 = pd.read_csv("resources/quality_of_life/2010_pop_by_CD.csv")

    return df.merge(pop_2010, left_on="CD_code", right_on="CD_code")
