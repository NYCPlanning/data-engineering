import pandas as pd

from aggregate.decennial_census import decennial_census_001020 as census_helpers
from utils import geo_helpers
from internal_review.set_internal_review_file import set_internal_review_files


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


# TODO: Next update, if we have data, this will need some tweaks.
# def pedestrian_hospitalizations(geography, write_to_internal_review=False):
#     raise Exception(
#         "No data was provided in the last EDDE updated for this indicator. Meanwhile, Assault Hospitalizations was refactored, and this will need to be fixed before being added back"
#     )
#     source_data = load_clean_source_data("pedestrian")
#     indicator_col_label = "safety_pedhospital_rate"

#     final = calculate_per100k_rate(source_data, geography)
#     final.name = indicator_col_label
#     final = pd.DataFrame(final)
#     if write_to_internal_review:
#         set_internal_review_files(
#             [(final, "pedestrian_hospitalizations.csv", geography)],
#             category="quality_of_life",
#         )

#     return final


CENSUS_POP_FIELD = "pop_20_count"
ONE_HUNDRED_K = 100000


def calculate_per100k_rate(source_data):
    # We need population numbers by PUMA, which we'll take from the census
    # TODO: check with Winnie/Pop about this. Eyeballing things, it doesn't look like anything changed drastically
    census = census_helpers.load_decennial_census_001020()[CENSUS_POP_FIELD]
    gb = source_data.join(census).groupby("geo_id").sum()

    return pd.DataFrame(
        (gb["count"] * (ONE_HUNDRED_K / gb[CENSUS_POP_FIELD]))
        .round(2)
        .replace({0: None})
    )


def _calc_geo_id(pd_row):
    match pd_row.geo_type:
        case "puma":
            borough_alpha = geo_helpers.borough_num_mapper[pd_row.Geography[0]]
            comm_dist_num = int(pd_row.Geography[1:3])
            return geo_helpers.community_district_to_puma(borough_alpha, comm_dist_num)
        case "borough":
            return geo_helpers.borough_num_mapper[pd_row.Geography]
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
    df = pd.read_csv(
        "resources/quality_of_life/non_fatal_assault_hospitalizations.csv",
        dtype={"Geography": str},
    ).rename(columns={"Number": "count"})
    df["geo_type"] = df["GeoType"].map(RAW_GEO_TYPE_MAPPER)
    df["geo_id"] = df.apply(_calc_geo_id, axis=1)

    return df[["geo_type", "geo_id", "count"]].set_index("geo_id")
