import pandas as pd
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import assign_PUMA_col, borough_name_mapper

from ingest.ingestion_helpers import read_from_S3


def count_residential_evictions(
    geography_level, write_to_internal_review=False, debug=False
):
    """Main accessor of indicator"""
    residential_evictions = load_residential_evictions(debug)
    aggregated_by_geography = aggregate_by_geography(
        residential_evictions, geography_level
    )
    if write_to_internal_review:
        set_internal_review_files(
            [(aggregated_by_geography, "evictions.csv", geography_level)],
            "housing_security",
        )
    return aggregated_by_geography


def load_residential_evictions(debug) -> pd.DataFrame:
    evictions = read_from_S3("doi_evictions", "housing_security")

    if debug:
        evictions = evictions.iloc[:1000, :]
    residential_evictions = evictions[
        evictions["residential/commercial"] == "Residential"
    ]
    residential_evictions["borough_name"] = (
        residential_evictions["borough"].str[0]
        + residential_evictions["borough"].str[1:].str.lower()
    )

    # Hot fix, can be made neater
    residential_evictions["borough_name"] = residential_evictions[
        "borough_name"
    ].replace({"Staten island": "Staten Island"})
    residential_evictions["borough"] = residential_evictions["borough_name"].map(
        borough_name_mapper
    )

    num_cols = ["latitude", "longitude"]
    for c in num_cols:
        residential_evictions[c] = pd.to_numeric(residential_evictions[c])

    return residential_evictions


def aggregate_by_geography(evictions, geography_level):
    assert geography_level in ["citywide", "borough", "puma"]
    if geography_level == "puma":
        evictions = assign_PUMA_col(
            evictions, "latitude", "longitude", geocode_process="from_eviction_address"
        )

        final = evictions.groupby(geography_level).size()

    if geography_level == "citywide":
        evictions["citywide"] = "citywide"
        final = evictions.groupby("citywide").size()
    if geography_level == "borough":
        final = evictions.groupby(geography_level).size()
    final.name = "evictions_count"
    final = pd.DataFrame(final)
    return final
