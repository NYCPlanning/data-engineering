import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from resources import load
from utils import geo_helpers

from aggregate.decennial_census.decennial_census_001020 import (
    load_decennial_census_001020,
)


def assault_hospitalizations(geography, write_to_internal_review=False):
    source_data = _load_assaults()
    final = calculate_per100k_rate(source_data[source_data["geo_type"] == geography])
    indicator_col_label = "safety_assaulthospital_rate"

    final.index.name = geography
    final.columns = [indicator_col_label]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "assault_hospitalizations.csv", geography)],
            category="quality_of_life",
        )
    return final


def pedestrian_hospitalizations(geography, write_to_internal_review=False):
    source_data = _load_pedestrians()
    final = calculate_per100k_rate(source_data[source_data["geo_type"] == geography])
    indicator_col_label = "safety_pedhospital_rate"

    final.index.name = geography
    final.columns = [indicator_col_label]

    if write_to_internal_review:
        set_internal_review_files(
            [(final, "pedestrian_hospitalizations.csv", geography)],
            category="quality_of_life",
        )
    return final


CENSUS_POP_FIELD = "pop_20_count"
ONE_HUNDRED_K = 100000


def calculate_per100k_rate(source_data):
    # We need population numbers by PUMA, which we'll take from the census
    # TODO: check with Winnie/Pop about this. Eyeballing things, it doesn't look like anything changed drastically
    census = load_decennial_census_001020()[CENSUS_POP_FIELD]
    gb = source_data.join(census).groupby("geo_id").sum()

    return pd.DataFrame(
        (gb["count"] * (ONE_HUNDRED_K / gb[CENSUS_POP_FIELD]))
        .round(2)
        .replace({0: None})
    )


def _calc_geo_id(pd_row):
    match pd_row.geo_type:
        case "puma":
            # GeoID format is XYY where X=borough (1-5), YY=community district
            geo_id_str = str(pd_row.GeoID)
            borough_num = geo_id_str[0]
            comm_dist_num = int(geo_id_str[1:])
            borough_alpha = geo_helpers.borough_num_mapper[borough_num]
            return geo_helpers.community_district_to_puma(borough_alpha, comm_dist_num)
        case "borough":
            return geo_helpers.borough_name_mapper[pd_row.Geography]
        case "citywide":
            return "citywide"
        case _:
            return ""


RAW_GEO_TYPE_MAPPER = {
    "CD": "puma",  # as in, this will _will_ be a puma row after processing
    "Borough": "borough",
    "Citywide": "citywide",
}


def _load_assaults():
    df = load("assault_hospitalizations").rename(columns={"Number": "count"})

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean count column: remove commas and convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["count"] = df["count"].astype(str).str.replace(",", "")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    return df[["geo_type", "geo_id", "count"]].set_index("geo_id")


def _load_pedestrians():
    df = load("pedestrian_hospitalizations").rename(columns={"Number": "count"})

    # Filter to the latest year only
    latest_year = df["TimePeriod"].max()
    df = df[df["TimePeriod"] == latest_year]

    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    # Clean count column: remove commas and convert suppressed values to None
    # "**" and "^^" are suppressed values in the source data
    df["count"] = df["count"].astype(str).str.replace(",", "")
    df["count"] = pd.to_numeric(df["count"], errors="coerce")

    return df[["geo_type", "geo_id", "count"]].set_index("geo_id")
