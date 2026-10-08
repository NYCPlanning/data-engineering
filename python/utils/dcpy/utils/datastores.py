"""Datastore-agnostic geospatial export helpers.

Writing a GeoDataFrame to a shapefile, FGDB, GeoPackage, or GeoParquet file has nothing to do with which
datastore (postgres, duckdb, ...) the data came from - these helpers are shared by both
dcpy.utils.postgres and dcpy.utils.duckdb's export paths.
"""

from pathlib import Path

import pandas as pd

from dcpy.utils.logging import logger

# Both single and multi variants are treated as the same geometry family so that a
# GEOMETRY column with mixed Point/MultiPoint (or Polygon/MultiPolygon) isn't silently
# split or dropped when filtering by geometry_type.
POINT_TYPES = ["Point", "MultiPoint"]
POLYGON_TYPES = ["Polygon", "MultiPolygon"]
LINE_TYPES = ["LineString", "MultiLineString"]


_GEOMETRY_FAMILIES = {
    "points": POINT_TYPES,
    "polygons": POLYGON_TYPES,
    "lines": LINE_TYPES,
}


def filter_geometry_type(gdf, geometry_type: str | None, label: str):
    """Keep only rows in one geometry family ("points" | "polygons" | "lines").

    Rows with null or empty geometry belong to no family and are dropped, with a
    warning, since they'd otherwise vanish from split exports without a trace.
    """
    if geometry_type is None:
        return gdf
    if geometry_type not in _GEOMETRY_FAMILIES:
        raise ValueError(
            f"Unknown geometry_type '{geometry_type}' for '{label}', "
            f"expected one of {sorted(_GEOMETRY_FAMILIES)}"
        )
    missing = gdf.geometry.isna() | gdf.geometry.is_empty
    if missing.any():
        logger.warning(
            f"Dropping {missing.sum()} row(s) with no geometry from '{label}' "
            f"(geometry_type={geometry_type})"
        )
    return gdf[gdf.geom_type.isin(_GEOMETRY_FAMILIES[geometry_type])]


def _normalize_to_single_geom_type(gdf, label: str):
    """Normalize a GDF to a single geometry type within a family.

    Source columns often contain a mix of Point/MultiPoint or Polygon/MultiPolygon.
    Both shapefiles and GDB layers require a single geometry type, so we promote to
    the multi-variant when both are present. Raises if types span different families
    (e.g. Point + Polygon), which requires an explicit geometry_type filter.
    """
    from shapely import MultiLineString, MultiPoint, MultiPolygon

    types_set = set(gdf.geom_type.dropna().unique())
    if types_set <= set(POINT_TYPES):
        # Exact equality avoids a no-op copy when already single-type.
        if types_set == {"Point", "MultiPoint"}:
            gdf = gdf.copy()
            gdf.geometry = gdf.geometry.apply(
                lambda g: MultiPoint([g]) if g.geom_type == "Point" else g
            )
    elif types_set <= set(POLYGON_TYPES):
        if types_set == {"Polygon", "MultiPolygon"}:
            gdf = gdf.copy()
            gdf.geometry = gdf.geometry.apply(
                lambda g: MultiPolygon([g]) if g.geom_type == "Polygon" else g
            )
    elif types_set <= set(LINE_TYPES):
        if types_set == {"LineString", "MultiLineString"}:
            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].apply(
                lambda g: MultiLineString([g]) if g.geom_type == "LineString" else g
            )
    else:
        raise ValueError(
            f"'{label}' contains geometry types from different families: "
            f"{sorted(types_set)}. Specify geometry_type='points', 'polygons', "
            "or 'lines' in custom."
        )
    return gdf


def write_shapefile_zip(gdf, table_name: str, file_path: Path, tmp_dir: Path) -> None:
    import shutil

    if gdf.empty:
        raise ValueError(
            f"No features to export for '{table_name}' shapefile "
            "(geometry_type filter returned zero rows)"
        )
    gdf = _normalize_to_single_geom_type(gdf, table_name)
    gdf.to_file(tmp_dir / table_name)
    shutil.make_archive(str(file_path.with_suffix("")), "zip", tmp_dir / table_name)


def write_geoparquet(gdf, table_name: str, file_path: Path) -> None:
    if gdf.empty:
        raise ValueError(
            f"No features to export for '{table_name}' geoparquet "
            "(geometry_type filter returned zero rows)"
        )
    gdf.to_parquet(file_path, index=False)


def _write_layers(
    layers: list[tuple[str, "pd.DataFrame"]],
    path: Path,
    driver: str,
    allow_empty: set[str] | None = None,
) -> None:
    """Write one or more named layers to a single multi-layer file (FGDB or GeoPackage).

    Each entry in `layers` is (layer_name, gdf). Layers are normalized to a single
    geometry type; use geometry_type='points' or 'polygons' in the recipe custom fields
    to filter before reaching here.

    An empty layer is an error by default, since it usually means a geometry_type
    filter matched nothing. Layers named in `allow_empty` (recipe `custom.allow_empty`)
    are written as schema-only instead, for feature classes that are legitimately
    empty upstream and still have to appear in the output.
    """
    import geopandas as gpd
    import pyogrio

    allow_empty = allow_empty or set()
    # The file is created on the first layer and the rest are appended. You can't write
    # multiple layers in one call. write_dataframe handles both GeoDataFrames (spatial,
    # one geometry type per layer) and plain DataFrames (non-spatial tables like
    # node_stname / altnames, which have no geometry column).
    for i, (layer_name, gdf) in enumerate(layers):
        write_kwargs: dict = {}
        if gdf.empty:
            if layer_name not in allow_empty:
                raise ValueError(f"No rows to export for layer '{layer_name}'")
            # An empty frame carries no geometry to infer from, so name the type.
            if isinstance(gdf, gpd.GeoDataFrame):
                write_kwargs["geometry_type"] = "MultiPolygon"
        elif isinstance(gdf, gpd.GeoDataFrame):
            gdf = _normalize_to_single_geom_type(gdf, layer_name)
        pyogrio.write_dataframe(
            gdf,
            str(path),
            driver=driver,
            layer=layer_name,
            append=i > 0,
            **write_kwargs,
        )


def write_gdb_zip(
    layers: list[tuple[str, "pd.DataFrame"]],
    file_path: Path,
    tmp_dir: Path,
    allow_empty: set[str] | None = None,
) -> None:
    """Write one or more named layers to a zipped FGDB. See `_write_layers`."""
    import shutil

    gdb_name = file_path.stem
    _write_layers(layers, tmp_dir / f"{gdb_name}.gdb", "OpenFileGDB", allow_empty)
    shutil.make_archive(
        str(file_path.with_suffix("")), "zip", tmp_dir, f"{gdb_name}.gdb"
    )


def write_gpkg(
    layers: list[tuple[str, "pd.DataFrame"]],
    file_path: Path,
    allow_empty: set[str] | None = None,
) -> None:
    """Write one or more named layers to a GeoPackage. See `_write_layers`.

    Not zipped: a GeoPackage is already a single file.
    """
    # The first layer is written with append=False, which would add to a stale file
    # rather than replace it.
    file_path.unlink(missing_ok=True)
    _write_layers(layers, file_path, "GPKG", allow_empty)
