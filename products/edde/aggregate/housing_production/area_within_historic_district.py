from typing import List
import geopandas as gp
from shapely import wkb
from utils.PUMA_helpers import puma_to_borough, PUMAS_2010
from ingest.ingestion_helpers import load_data


supported_geographies = ["puma", "borough", "citywide"]


def _rename_col(cols) -> List:
    new_cols = [col if "pct" in col else col + "_count" for col in cols]

    return new_cols


def _generate_geographies(geography_level):
    NYC_PUMAs = PUMAS_2010.to_crs("EPSG:2263")
    if geography_level == "puma":
        return NYC_PUMAs.set_index("puma")
    if geography_level == "borough":
        NYC_PUMAs["borough"] = NYC_PUMAs.apply(axis=1, func=puma_to_borough)
        by_borough = NYC_PUMAs.dissolve(by="borough")
        return by_borough
    if geography_level == "citywide":
        citywide = NYC_PUMAs.dissolve()
        citywide.index = ["citywide"]
        return citywide

    raise Exception(f"Supported geographies are {supported_geographies}")


def _fraction_PUMA_historic(PUMA, hd):
    gdf = gp.GeoDataFrame(geometry=[PUMA.geometry], crs="EPSG:2263")
    overlay = gp.overlay(hd, gdf, "intersection")
    if overlay.empty:
        return 0, 0
    else:
        fraction = (overlay.area.sum() / gdf.geometry.area.sum()) * 100
    return fraction, overlay.area.sum() / (5280**2)


def _load_historic_districts_gdf() -> gp.GeoDataFrame:
    df = load_data("lpc_historic_district_areas")

    hd = gp.GeoDataFrame(df)
    hd["the_geom"] = hd["wkb_geometry"].apply(wkb.loads)
    hd.set_geometry(col="the_geom", inplace=True, crs="EPSG:4326")
    hd = hd.explode(column="the_geom", index_parts=True)
    hd.set_geometry("the_geom", inplace=True)
    hd = hd.to_crs("EPSG:2263")
    hd = hd.reset_index()
    return hd


def fraction_historic(geography_level):
    gdf = _generate_geographies(geography_level)
    gdf["total_sqmiles"] = gdf.geometry.area / (5280**2)
    hd = _load_historic_districts_gdf()
    gdf[["area_historic_pct", "area_historic_sqmiles"]] = gdf.apply(
        _fraction_PUMA_historic, axis=1, args=(hd,), result_type="expand"
    )
    gdf.columns = _rename_col(gdf.columns)
    return gdf[
        ["area_historic_sqmiles_count", "area_historic_pct", "total_sqmiles_count"]
    ].round(2)
