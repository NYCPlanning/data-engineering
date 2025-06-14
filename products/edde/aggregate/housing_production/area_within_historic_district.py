import geopandas as gp
from ingest.ingestion_helpers import load_data
from utils.geo_helpers import get_2020_pumas


supported_geographies = ["puma", "borough", "citywide"]


def _load_historic_districts_gdf() -> gp.GeoDataFrame:
    return (
        load_data("lpc_historic_district_areas", is_geospatial=True)
        .to_crs(  # type: ignore
            "EPSG:2263"
        )
        .explode(column="wkb_geometry")
    )


def _generate_geographies(geography_level):
    pumas = get_2020_pumas()

    if geography_level == "puma":
        return pumas.set_index("puma")
    if geography_level == "borough":
        by_borough = pumas.dissolve(by="borough")
        return by_borough
    if geography_level == "citywide":
        citywide = pumas.dissolve()
        citywide["citywide"] = "citywide"
        return citywide.set_index("citywide")

    raise Exception(f"Supported geographies are {supported_geographies}")


def _fraction_PUMA_historic(puma, hd):
    gdf = gp.GeoDataFrame(geometry=[puma.geom], crs="EPSG:2263")
    overlay = gp.overlay(hd, gdf, "intersection")
    if overlay.empty:
        return 0, 0
    else:
        fraction = (overlay.area.sum() / gdf.geometry.area.sum()) * 100
    return fraction, overlay.area.sum() / (5280**2)


def fraction_historic(geography_level):
    hd = _load_historic_districts_gdf()

    puma_geos = _generate_geographies(geography_level)
    puma_geos["total_sqmiles"] = puma_geos.geom.area / (5280**2)

    puma_geos[["area_historic_pct", "area_historic_sqmiles"]] = puma_geos.apply(
        _fraction_PUMA_historic, axis=1, args=(hd,), result_type="expand"
    )
    puma_geos.columns = [
        col if "pct" in col else col + "_count" for col in puma_geos.columns
    ]

    return puma_geos[
        ["area_historic_sqmiles_count", "area_historic_pct", "total_sqmiles_count"]
    ].round(2)
