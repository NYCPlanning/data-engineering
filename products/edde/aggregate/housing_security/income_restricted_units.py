import pandas as pd
from resources import load
from utils.data_loaders import load_data
from utils.geo_helpers import (
    borough_name_mapper,
    clean_PUMAs,
    filter_for_recognized_pumas,
    puma_to_borough,
)

from aggregate.load_aggregated import initialize_dataframe_geo_index


def _load_clean_income_restricted():
    source_data = load("nycha_tenants")
    source_data.rename(columns={"PUMA (2020)": "puma"}, inplace=True)
    source_data["puma"] = source_data["puma"].apply(clean_PUMAs)
    source_data = filter_for_recognized_pumas(source_data)
    source_data["borough"] = source_data.apply(axis=1, func=puma_to_borough)
    source_data["citywide"] = "citywide"

    source_data.rename(
        columns={"Total Unit Count": "units_nycha_count"},
        inplace=True,
    )
    return source_data


def income_restricted_units(geography: str) -> pd.DataFrame:
    assert geography in ["puma", "borough", "citywide"]

    source_data = _load_clean_income_restricted()
    empty_df = initialize_dataframe_geo_index(geography=geography)
    final = source_data.groupby(geography).sum()[["units_nycha_count"]]
    final = pd.concat([empty_df, final], axis=1)
    final.fillna(0, inplace=True)
    return final


_HPD_COLUMNS = [
    "project_id",
    "project_name",
    "project_start_date",
    "project_completion_date",
    "number",
    "street",
    "borough",
    "community_board",
    "nta_-_neighborhood_tabulation_area",
    "latitude_(internal)",
    "longitude_(internal)",
    "all_counted_units",
]


def _load_clean_hpd_data():
    import geopandas as gpd
    from utils.geo_helpers import puma_from_point

    source_data = load_data(
        "hpd_hny_units_by_building",
        cols=_HPD_COLUMNS,
    )
    # casting to numeric for calculation
    for c in ["latitude_(internal)", "longitude_(internal)", "all_counted_units"]:
        source_data[c] = pd.to_numeric(source_data[c])

    source_data.rename(
        columns={
            "nta_-_neighborhood_tabulation_area": "nta",
            "all_counted_units": "units_hpd_count",
        },
        inplace=True,
    )
    source_data["borough"] = source_data["borough"].map(borough_name_mapper)
    source_data["citywide"] = "citywide"

    # Use geocoding to assign PUMAs instead of NTA join
    # Filter to records with valid internal coordinates
    has_coords = (
        source_data["latitude_(internal)"].notna()
        & source_data["longitude_(internal)"].notna()
    )

    # Create geometry for geocoding
    gdf = gpd.GeoDataFrame(
        source_data[has_coords].copy(),
        geometry=gpd.points_from_xy(
            source_data[has_coords]["longitude_(internal)"],
            source_data[has_coords]["latitude_(internal)"],
        ),
        crs="EPSG:4326",
    ).to_crs("EPSG:2263")

    # Assign PUMAs using point-in-polygon
    gdf["puma"] = gdf.geometry.apply(puma_from_point)

    # Drop geometry column to return regular DataFrame
    source_data_with_puma = pd.DataFrame(gdf.drop(columns=["geometry"]))

    return source_data_with_puma


def income_restricted_units_hpd(geography: str) -> pd.DataFrame:
    assert geography in ["puma", "borough", "citywide"]

    source_data = _load_clean_hpd_data()
    final = source_data.groupby(geography).sum()[["units_hpd_count"]]
    return final
