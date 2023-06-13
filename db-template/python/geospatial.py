import contextily as cx
import geopandas as gpd
import pandas as pd
from folium.folium import Map, TileLayer
from matplotlib.axes import Axes
from xyzservices import TileProvider

NYC_PROJECTION = "EPSG:2263"
WKT_PROJECTION = "EPSG:4326"

DEFAULT_BASEMAP = cx.providers.Stamen.Terrain
DEFAULT_MATPLOTLIB_MAP_CONFIG = {
    "figsize": (10, 10),
    "alpha": 0.5,
    "edgecolor": "g",
    "linewidths": 3,
}
DEFAULT_FOLIUM_MAP_CONFIG = {
    "tiles": "CartoDB positron",
    "color": "green",
    "style_kwds": {
        "weight": 4,
    },
}


def convert_to_geodata(
    data: pd.DataFrame,
    geometry_column: str = "geometry",
    projection: str = WKT_PROJECTION,
) -> gpd.GeoDataFrame:
    data[geometry_column] = gpd.GeoSeries.from_wkt(data[geometry_column])
    data = gpd.GeoDataFrame(data, crs=projection)

    return data


def reproject_geometry(
    data: gpd.GeoDataFrame, old_projection: str, new_projection: str
) -> gpd.GeoDataFrame:
    if not data.crs:
        print(
            f"GeoDataFrame has no CRS set. assigning declared old projection of {old_projection} ..."
        )
        data.set_crs(old_projection, inplace=True)
    print(f"reporjecting from {old_projection} to {new_projection} ...")
    data.to_crs(new_projection, inplace=True)
    if data.crs != new_projection:
        raise RuntimeError(
            f"Actual new projection {data.crs} is not the expected {new_projection}"
        )
    return data


def pad_map_bounds(
    data: gpd.GeoDataFrame,
    axes: Axes,
    x_scale: float,
    y_scale: float,
) -> Axes:
    lon_min, lat_min, lon_max, lat_max = data.total_bounds
    lon_pad, lat_pad = y_scale * (lon_max - lon_min), x_scale * (lat_max - lat_min)

    axes.axis("scaled")
    axes.axis(
        [lon_min - lon_pad, lon_max + lon_pad, lat_min - lat_pad, lat_max + lat_pad]
    )
    return axes


def map_simple(
    data: gpd.GeoDataFrame,
    projection: str = NYC_PROJECTION,
    basemap: str | TileProvider = DEFAULT_BASEMAP,
    map_config: dict = DEFAULT_MATPLOTLIB_MAP_CONFIG,
) -> Axes:
    axes = data.plot(
        **map_config,
    )
    cx.add_basemap(
        axes,
        crs=projection,
        source=basemap,
    )
    axes.set_axis_off()

    return axes


def map_folium(
    data: gpd.GeoDataFrame, map_config: dict = DEFAULT_FOLIUM_MAP_CONFIG
) -> Map:
    # GeoDataFrame.explore fails if any columns have a dtype of object
    for column in data.columns:
        if isinstance(column, object) and column != "geometry":
            data[column] = data[column].astype(str)

    map_figure = data.explore(**map_config)

    return map_figure
