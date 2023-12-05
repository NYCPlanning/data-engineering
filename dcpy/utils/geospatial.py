from numpy import floor
import pandas as pd
import geopandas as gpd
import shapely
import leafmap.foliumap as lmf
from osgeo import gdal
import typer
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from dcpy.utils.logging import logger

GEOMETRY_FORMATS = {
    "WKT": "EPSG:4326",
    "WKB": "ESPG:3867",
}
PROJECTIONS = {
    "NY_LONG_ISLAND": "EPSG:2263",
    "WGS_84_DEG": "EPSG:4326",
    "WGS_84_METERS": "EPSG:4087",
}

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


def convert_to_geodata(
    data: pd.DataFrame,
    geometry_format: str,
    geometry_column: str,
    new_geometry_column: str = "geometry",
) -> gpd.GeoDataFrame:
    data = data.copy()
    match geometry_format:
        case "WKT":
            data[geometry_column] = gpd.GeoSeries.from_wkt(data[geometry_column])
        case "WKB":

            def _try_wkb(wkb):
                try:
                    return pd.Series([shapely.from_wkb(wkb), None])
                except Exception as e:
                    return pd.Series([None, str(e)])

            data[[new_geometry_column, "geometry_error"]] = data.apply(
                lambda row: _try_wkb(row[geometry_column]), axis=1
            )
        case _:
            raise NotImplementedError(
                f"Unsupported geometry format {geometry_format:}. Must be one of the keys in the dict {GEOMETRY_FORMATS:}"
            )
    data.drop([geometry_column], axis=1, inplace=True)
    geo_data = gpd.GeoDataFrame(
        data,
        geometry=new_geometry_column,
        crs=PROJECTIONS["WGS_84_DEG"],
    )
    return geo_data


def reproject_geodata(data: gpd.GeoDataFrame, new_crs: str) -> gpd.GeoDataFrame:
    return data.to_crs(new_crs)


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


app = typer.Typer(add_completion=False)


@app.command()
def _cli_wrapper_translate(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
    ),
    input_path: str = typer.Option(
        None,
        "-i",
        "--input-path",
    ),
    min_zoom: int = typer.Option(
        None,
        "--min-zoom",
    ),
    max_zoom: int = typer.Option(
        None,
        "--max-zoom",
    ),
):
    translate_shp_to_mvt(product, input_path, min_zoom, max_zoom)


if __name__ == "__main__":
    app()
