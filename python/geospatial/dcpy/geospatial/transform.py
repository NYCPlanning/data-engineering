import geopandas as gpd
import pandas as pd
from shapely import (
    Geometry,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)

from dcpy.utils import formats as file
from dcpy.utils import geometry as geom


def multi(geom: Geometry | None) -> Geometry | None:
    match geom:
        case Point():
            return MultiPoint([geom])
        case LineString():
            return MultiLineString([geom])
        case Polygon():
            return MultiPolygon([geom])
        case _:
            return geom


def df_to_gdf(df: pd.DataFrame, geometry: file.Geometry) -> gpd.GeoDataFrame:
    """
    Convert a pandas DataFrame to a GeoDataFrame based on the provided geometry information.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    geometry (file.Geometry): An object containing geometry information.

    Returns:
    gpd.GeoDataFrame: The resulting GeoDataFrame.
    """

    # case when geometry is in one column (i.e. polygon or point object type)
    if isinstance(geometry.geom_column, str):
        geom_column = geometry.geom_column
        assert geom_column in df.columns, (
            "❌ Specified geometry column does not exist in the dataset."
        )

        match geometry.format:
            case None | geom.StandardGeometryFormat.wkt:
                df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])
            case geom.StandardGeometryFormat.wkb:
                df[geom_column] = gpd.GeoSeries.from_wkb(df[geom_column])
            case geom.PointXYStr():
                df[geom_column] = df[geom_column].apply(geometry.format.wkt)
                df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])
            case _:
                raise ValueError(
                    f"Unsupported geometry column format {geometry.format}"
                )

        gdf = gpd.GeoDataFrame(
            df,
            geometry=geom_column,
            crs=geometry.crs,
        )
    # case when geometry is specified as lon and lat columns
    else:
        x_column = geometry.geom_column.x
        y_column = geometry.geom_column.y
        assert x_column in df.columns and y_column in df.columns, (
            "❌ Longitude or latitude columns specified do not exist in the dataset."
        )

        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df[x_column], df[y_column]),
            crs=geometry.crs,
        )
        # maybe bug in shapely 2.1 - Point(NaN NaN) no longer returns true with .is_empty
        # regardless, only way a point can be invalid is if one of coordinates is invalid
        gdf.geometry = gdf.geometry.apply(lambda geom: geom if geom.is_valid else None)

    return gdf


def reproject_gdf(
    df: gpd.GeoDataFrame, target_crs: str, source_crs: str | None = None
) -> gpd.GeoDataFrame:
    if not df.crs:
        if not source_crs:
            raise ValueError("df has no crs set and none provided")
        df.set_crs(source_crs, inplace=True)
    else:
        if source_crs and (source_crs != df.crs.srs):
            raise ValueError(
                f"source crs '{source_crs}' supplied, but gdf has crs '{df.crs.srs}'."
            )
    return df.to_crs(target_crs)
