import usaddress
import pandas as pd

from ingest import ingestion_helpers
from internal_review.set_internal_review_file import set_internal_review_files
from utils.PUMA_helpers import borough_name_mapper, puma_from_coord
from utils.geocode import get_geosupport_puma

report_years = {"2019", "2020", "2021", "2022", "2023", "2024"}


def _load_residential_evictions() -> pd.DataFrame:
    evictions = ingestion_helpers.load_data("doi_evictions")
    residential_evictions = evictions[
        evictions["residential/commercial"] == "Residential"
    ].copy()

    # Filter down to just our report years
    residential_evictions["year"] = residential_evictions.executed_date.apply(
        lambda x: x[-4:]
    )
    residential_evictions = residential_evictions[
        residential_evictions["year"].isin(report_years)
    ].copy()

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


def _from_geocoded_eviction_address(record) -> str:
    """Return latitude, longitude in degrees"""
    """Using these docs as guide https://usaddress.readthedocs.io/en/latest/"""
    parsed = usaddress.parse(record.eviction_address)
    parsed = {k: v for v, k in parsed}
    rv = {}
    rv["address_num"] = parsed.get("AddressNumber", "")
    street_name_components = [
        parsed.get("StreetNamePreModifier"),
        parsed.get("StreetNamePreDirectional"),
        parsed.get("StreetNamePreType"),
        parsed.get("StreetName"),
        parsed.get("StreetNamePostModifier"),
        parsed.get("StreetNamePostDirectional"),
        parsed.get("StreetNamePostType"),
    ]
    rv["street_name"] = " ".join([s for s in street_name_components if s])
    rv["borough"] = record.borough
    rv["zip"] = record.eviction_postcode

    return get_geosupport_puma(rv)


def _get_puma(df_record) -> str | None:
    if pd.notnull(df_record.latitude) and pd.notnull(df_record.longitude):
        return puma_from_coord(
            longitude=df_record.longitude, latitude=df_record.latitude
        )
    else:
        return _from_geocoded_eviction_address(df_record)


def _aggregate_by_geography(evictions, geography_level):
    assert geography_level in ["citywide", "borough", "puma"]
    if geography_level == "puma":
        evictions["puma"] = evictions.apply(_get_puma, axis=1)
    elif geography_level == "citywide":
        evictions["citywide"] = "citywide"
        # final = evictions.groupby("citywide").size()
    final = evictions.groupby(geography_level).size()
    final.name = "evictions_count"

    return pd.DataFrame(final)


def count_residential_evictions(geography_level, write_to_internal_review=False):
    residential_evictions = _load_residential_evictions()
    aggregated_by_geography = _aggregate_by_geography(
        residential_evictions, geography_level
    )
    if write_to_internal_review:
        set_internal_review_files(
            [(aggregated_by_geography, "evictions.csv", geography_level)],
            "housing_security",
        )
    return aggregated_by_geography
