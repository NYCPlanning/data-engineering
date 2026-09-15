"""Write point data out as GeoJSON and Shapefile.

GeoJSON keeps the source records intact. Shapefile cannot: the format caps field names at
10 characters, text values at 254, and has no nested types. Rather than let those limits
apply themselves silently, `write_shapefile` renames deterministically, serializes nested
values to JSON, reports every truncation, and writes a `*_fields.csv` mapping so a reader
can get back to the original field names.
"""

import json
import re
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import typer

from dcpy.utils.logging import logger

# The sidecar files a shapefile is made of. Named explicitly rather than globbed on the
# stem, which also matches a same-named .geojson sitting in the output directory.
SHAPEFILE_PARTS = (".shp", ".shx", ".dbf", ".prj", ".cpg", ".sbn", ".sbx")
SHAPEFILE_NAME_LIMIT = 10
SHAPEFILE_VALUE_LIMIT = 254
WGS84 = "EPSG:4326"

app = typer.Typer(add_completion=False)


def _abbreviate(name: str, limit: int = SHAPEFILE_NAME_LIMIT) -> str:
    """Shorten a camelCase name to `limit` chars, keeping every word recognizable.

    Plain truncation maps all five `parcelLocker*` fields onto `parcelLock`. Splitting on
    word boundaries and giving each word a share of the budget keeps them distinct
    (`parLocAddr`, `parLocCity`, ...) without needing a hand-written table.
    """
    if len(name) <= limit:
        return name
    words = re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+", name) or [name]
    share, extra = divmod(limit, len(words))
    if share == 0:  # more words than characters; fall back to initials
        return "".join(w[0] for w in words)[:limit]
    return "".join(
        word[: share + (1 if i < extra else 0)] for i, word in enumerate(words)
    )


def shapefile_field_names(columns: list[str]) -> dict[str, str]:
    """Map each column to a unique shapefile-legal field name."""
    names: dict[str, str] = {}
    used: set[str] = set()
    for column in columns:
        candidate = _abbreviate(column)
        if candidate in used:
            # Distinct sources can still abbreviate alike; number them rather than let
            # the driver silently overwrite one with the other.
            for i in range(1, 100):
                suffix = str(i)
                candidate = _abbreviate(column, SHAPEFILE_NAME_LIMIT - len(suffix))
                candidate = f"{candidate}{suffix}"
                if candidate not in used:
                    break
            else:
                raise ValueError(f"Cannot find a unique shapefile name for {column!r}")
        names[column] = candidate
        used.add(candidate)
    return names


def read_points(
    path: Path,
    *,
    latitude_column: str = "latitude",
    longitude_column: str = "longitude",
    crs: str = WGS84,
) -> gpd.GeoDataFrame:
    """Read a JSON array of records into a point GeoDataFrame."""
    records = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame.from_records(records)
    missing = {latitude_column, longitude_column} - set(df.columns)
    if missing:
        raise ValueError(f"{path} has no {sorted(missing)} column(s)")
    # The coordinates arrive as strings; anything unparseable would otherwise become a
    # point at (0, 0) off the coast of Africa rather than an error.
    lat = pd.to_numeric(df[latitude_column], errors="coerce")
    lon = pd.to_numeric(df[longitude_column], errors="coerce")
    if bad := int((lat.isna() | lon.isna()).sum()):
        raise ValueError(f"{bad} of {len(df)} records have unparseable coordinates")
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat), crs=crs)


def _jsonable(value):
    """Convert numpy/pandas scalars to plain Python, leaving nested values alone."""
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, np.generic):
        return value.item()
    return None if value is None or value is pd.NA or value != value else value


def write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> Path:
    """Write GeoJSON in WGS84, as the format's spec requires.

    Built by hand rather than via `to_file`: GDAL's GeoJSON writer renders a nested
    property through Python's `repr`, so a list of dicts lands in the file as
    "[{'day': 'MO'}]" - single-quoted, `None` for null, and not parseable as JSON by
    anything. GeoJSON permits arbitrary JSON in `properties`, so nesting is kept intact.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    wgs84 = gdf.to_crs(WGS84)
    records = wgs84.drop(columns=[wgs84.geometry.name]).to_dict(orient="records")
    features = [
        {
            "type": "Feature",
            "geometry": shapely.geometry.mapping(geom) if geom is not None else None,
            "properties": {k: _jsonable(v) for k, v in record.items()},
        }
        for record, geom in zip(records, wgs84.geometry)
    ]
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
        encoding="utf-8",
    )
    logger.info(f"Wrote {len(features)} features to {path}")
    return path


def write_shapefile(gdf: gpd.GeoDataFrame, path: Path, *, crs: str = WGS84) -> Path:
    """Write a shapefile plus a CSV mapping its field names back to the originals."""
    path.parent.mkdir(parents=True, exist_ok=True)
    flat = gdf.to_crs(crs).copy()

    columns = [c for c in flat.columns if c != flat.geometry.name]
    for column in columns:
        if flat[column].map(lambda v: isinstance(v, (list, dict))).any():
            flat[column] = flat[column].map(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )
        overflow = flat[column].map(lambda v: len(v) if isinstance(v, str) else 0)
        if (longest := int(overflow.max())) > SHAPEFILE_VALUE_LIMIT:
            logger.warning(
                f"{column!r} holds values up to {longest} chars; the shapefile copy is "
                f"cut to {SHAPEFILE_VALUE_LIMIT}. The GeoJSON keeps them whole."
            )

    names = shapefile_field_names(columns)
    flat = flat.rename(columns=names)
    flat.to_file(path, driver="ESRI Shapefile")

    fields_path = path.with_name(f"{path.stem}_fields.csv")
    pd.DataFrame(
        sorted(names.items()), columns=["source_field", "shapefile_field"]
    ).to_csv(fields_path, index=False)
    renamed = sum(1 for source, short in names.items() if source != short)
    logger.info(
        f"Wrote {len(flat)} features to {path} ({renamed} of {len(names)} fields "
        f"renamed; mapping in {fields_path.name})"
    )

    # A shapefile is five-plus files that are useless apart; zip it so it can be handed
    # over as one attachment, field mapping included.
    archive = path.with_name(f"{path.stem}_shapefile.zip")
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
        for suffix in SHAPEFILE_PARTS:
            part = path.with_suffix(suffix)
            if part.exists():
                zf.write(part, part.name)
        zf.write(fields_path, fields_path.name)
    logger.info(f"Zipped shapefile to {archive}")
    return path


@app.command()
def convert(
    input_path: Path = typer.Argument(..., help="JSON array of point records."),
    output_dir: Path = typer.Option(
        None, "--output-dir", "-o", help="Defaults to the input file's directory."
    ),
    latitude_column: str = typer.Option("latitude", "--latitude"),
    longitude_column: str = typer.Option("longitude", "--longitude"),
    shapefile_crs: str = typer.Option(
        WGS84, "--shapefile-crs", help="e.g. EPSG:2263 for NY State Plane feet."
    ),
) -> None:
    """Write <input>.geojson and <input>.shp beside the input JSON."""
    output_dir = output_dir or input_path.parent
    gdf = read_points(
        input_path,
        latitude_column=latitude_column,
        longitude_column=longitude_column,
    )
    write_geojson(gdf, output_dir / f"{input_path.stem}.geojson")
    write_shapefile(gdf, output_dir / f"{input_path.stem}.shp", crs=shapefile_crs)


if __name__ == "__main__":
    app()
