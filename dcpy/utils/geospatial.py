from numpy import floor
from enum import Enum
import geopandas as gpd
import json
import leafmap.foliumap as lmf
import pandas as pd
from pathlib import Path
from pyarrow import parquet
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
import shapely
from typing import Callable

from dcpy.models.geospatial import parquet as geoparquet
from dcpy.utils.logging import logger


DEFAULT_MAP_SIZE = {"width": 900, "height": 700}
DEFAULT_BASEMAP = "Stadia.AlidadeSmoothDark"
NYC_CENTER_COORDS = [40.726446, -73.983307]
NYC_CENTER_ZOOM = 13

PRODUCT_MVT_FIELDS = {
    "pluto": [
        "BBL",
        "LandUse",
        "LotType",
        "NumBldgs",
    ]
}


class GeometryFormat(str, Enum):
    wkt = "WKT"
    wkb = "WKB"


class GeometryCRS(str, Enum):
    wgs_84_deg = "EPSG:4326"
    wgs_84_meters = "EPSG:4087"
    ny_long_island = "EPSG:2263"


def convert_to_geodata(
    data: pd.DataFrame,
    geometry_column: str,
    geometry_format: GeometryFormat,
    crs: GeometryCRS,
) -> gpd.GeoDataFrame:
    data = data.copy()
    new_geometry_column = "geometry_generated"

    def _create_geometries(
        geometry_column: str, to_geoseries_function: Callable
    ) -> pd.Series:
        # gpd.GeoSeries.from_wkb and from_wkt fail if any rows have no geometry
        try:
            return pd.Series([to_geoseries_function(geometry_column), None])
        except Exception as e:
            return pd.Series([None, str(e)])

    match geometry_format:
        case GeometryFormat.wkt:
            to_geoseries_function = shapely.from_wkt
        case GeometryFormat.wkb:
            to_geoseries_function = shapely.from_wkb
        case _:
            raise NotImplementedError(
                f"Geometry format {geometry_format:} has not been implemented."
            )
    data[[new_geometry_column, "geometry_error"]] = data.apply(
        lambda row: _create_geometries(row[geometry_column], to_geoseries_function),
        axis=1,
    )
    geo_data = gpd.GeoDataFrame(
        data,
        geometry=new_geometry_column,
        crs=crs.value,
    )
    return geo_data


def generate_folium_map(gdf: gpd.GeoDataFrame) -> lmf.Map:
    basic_map = lmf.Map(
        center=NYC_CENTER_COORDS,
        zoom=NYC_CENTER_ZOOM,
    )
    basic_map.add_basemap(DEFAULT_BASEMAP)

    gdf_points = gdf[gdf.geom_type == "Point"]
    gdf_points["latitude"] = gdf_points.centroid.y
    gdf_points["longitude"] = gdf_points.centroid.x
    gdf_polygons = gdf[gdf.geom_type == "Polygon"]
    gdf_multipolygons = gdf[gdf.geom_type == "MultiPolygon"]

    if len(gdf_points) > 0:
        basic_map.add_points_from_xy(
            gdf_points, crs=str(gdf_points.crs), layer_name=f"Points"
        )
    if len(gdf_polygons) > 0:
        basic_map.add_gdf(gdf_polygons, layer_name=f"Polygons")
    if len(gdf_multipolygons) > 0:
        basic_map.add_gdf(
            gdf_multipolygons,
            layer_name=f"MultiPolygons",
        )

    return basic_map


def translate_shp_to_mvt(
    product: str, input_path: str, min_zoom: int = 0, max_zoom: int = 5
) -> None:
    """Keeping scope of this very limited - should be refactored once data library is fully brought in"""
    from osgeo import gdal

    output_path = f"{product}_mvt"
    select_fields = (
        PRODUCT_MVT_FIELDS[product] if product in PRODUCT_MVT_FIELDS.keys() else None
    )
    logger.info(f"Generating MVTs for {product} using {input_path}")
    logger.info(f"Based on product name, limited to fields {select_fields}")

    with Progress(
        SpinnerColumn(spinner_name="earth"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        transient=False,
    ) as progress:
        task = progress.add_task(
            f"[green]Translating [bold]{input_path}[/bold]", total=1000
        )

        def update_progress(complete, message, unknown):
            progress.update(task, completed=floor(complete * 1000))

        gdal.UseExceptions()
        gdal.VectorTranslate(
            output_path,
            f"/vsizip/{input_path}",
            format="MVT",
            accessMode="overwrite",
            makeValid=True,
            selectFields=select_fields,
            # optional settings
            callback=update_progress,
            datasetCreationOptions=[f"MINZOOM={min_zoom}", f"MAXZOOM={max_zoom}"],
        )


def read_parquet_metadata(filepath: Path) -> geoparquet.MetaData:
    """
    Given filepath to GeoParquet file, returns both standard pyarrow parquet FileMetaData
    And geospatial metadata as defined in GeoParquet spec
    """
    parquet_metadata = parquet.read_metadata(filepath)
    if geoparquet.GEOPARQUET_METADATA_KEY not in parquet_metadata.metadata:
        raise TypeError(f"{filepath} is not a geoparquet file.")
    geo = parquet_metadata.metadata[geoparquet.GEOPARQUET_METADATA_KEY]
    geo_parquet = geoparquet.GeoParquet(**json.loads(geo))
    return geoparquet.MetaData(file_metadata=parquet_metadata, geo_parquet=geo_parquet)
