import geopandas as gpd
import pandas as pd
from utils import data_loaders
from utils.geo_helpers import puma_from_point


def _load_residential_evictions() -> gpd.GeoDataFrame:
    """
    Load geocoded residential evictions data.

    Uses the pre-geocoded doi_evictions_geocoded dataset which has:
    - Geocoded coordinates for missing lat/lon
    - PUMA codes where available
    - Current year already filtered out
    - Borough names already cleaned

    Filters to evictions from 2019 onwards.
    """
    evictions = data_loaders.load_data("doi_evictions_geocoded", is_geospatial=True)

    # Filter to residential only
    residential_evictions = evictions[
        evictions["residential/commercial"] == "Residential"
    ].copy()

    # Filter to 2019 onwards using the year column
    # (year column is already populated by the geocoding script)
    if "year" in residential_evictions.columns:
        residential_evictions = residential_evictions[
            residential_evictions["year"].astype(int) >= 2019
        ].copy()

    # Create GeoDataFrame with geometry
    # Filter out records without valid coordinates
    has_coords = (
        residential_evictions["latitude"].notna()
        & residential_evictions["longitude"].notna()
    )
    residential_evictions = residential_evictions[has_coords].copy()

    residential_evictions = gpd.GeoDataFrame(
        residential_evictions,
        geometry=gpd.points_from_xy(
            residential_evictions.longitude, residential_evictions.latitude
        ),
        crs="EPSG:4326",
    ).to_crs("EPSG:2263")

    return residential_evictions


def _get_puma(df_record) -> str | None:
    """
    Get PUMA for a record.

    Uses pre-computed PUMA if available (from Geosupport geocoding),
    otherwise computes from geometry point.
    """
    if pd.notnull(df_record.get("puma")):
        return df_record["puma"]
    elif pd.notnull(df_record.latitude) and pd.notnull(df_record.longitude):
        return puma_from_point(df_record.geometry)
    else:
        return None


def _aggregate_by_geography(evictions, geography_level):
    """
    Aggregate evictions by geography level.

    Args:
        evictions: GeoDataFrame of evictions
        geography_level: One of "citywide", "borough", or "puma"
    """
    assert geography_level in ["citywide", "borough", "puma"]

    evictions = evictions.copy()

    if geography_level == "puma":
        # Compute PUMA for records that don't have it
        needs_puma = evictions["puma"].isna()
        if needs_puma.any():
            evictions.loc[needs_puma, "puma"] = evictions[needs_puma].apply(
                _get_puma, axis=1
            )
    elif geography_level == "citywide":
        evictions["citywide"] = "citywide"

    final = evictions.groupby(geography_level).size()
    final.name = "evictions_count"

    return pd.DataFrame(final)


def count_residential_evictions(geography_level):
    """
    Count residential evictions by geography level.

    Uses the doi_evictions_geocoded dataset which already excludes the current year
    (incomplete data) so all historical years are included.

    Args:
        geography_level: One of "citywide", "borough", or "puma"

    Returns:
        DataFrame with eviction counts by geography
    """
    residential_evictions = _load_residential_evictions()

    aggregated_by_geography = _aggregate_by_geography(
        residential_evictions, geography_level
    )
    return aggregated_by_geography
