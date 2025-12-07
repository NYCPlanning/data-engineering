from numpy import floor
import geopandas as gpd
import pandas as pd
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from shapely import (
    Geometry,
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
)

from dcpy.models import file
from dcpy.models.geospatial import geometry as geom
from dcpy.utils.logging import logger


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

        # replace NaN values with None. Otherwise gpd throws an error
        if df[geom_column].isnull().any():
            df[geom_column] = df[geom_column].astype(object)
            df[geom_column] = df[geom_column].where(df[geom_column].notnull(), None)  # type: ignore

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


def translate_shp_to_mvt(
    product: str,
    input_path: str,
    mvt_fields: list[str] | None = None,
    min_zoom: int = 0,
    max_zoom: int = 5,
) -> None:
    """Keeping scope of this very limited - should be refactored once data library is fully brought in"""
    from osgeo import gdal

    output_path = f"{product}_mvt"
    logger.info(f"Generating MVTs for {product} using {input_path}")
    logger.info(f"Based on product name, limited to fields {mvt_fields}")

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
            selectFields=mvt_fields,
            # optional settings
            callback=update_progress,
            datasetCreationOptions=[f"MINZOOM={min_zoom}", f"MAXZOOM={max_zoom}"],
        )
