from pathlib import Path
from typing import Literal

from overturemaps import cli as overturemaps_cli
from overturemaps.core import record_batch_reader, type_theme_map

from dcpy.connectors.registry import Connector

BASE_URL = "az://overturemapswestus2.blob.core.windows.net"

RELEASE = "2025-10-22.0"

OvertureType = Literal[
    "address",
    "bathymetry",
    "building",
    "building_part",
    "division",
    "division_area",
    "division_boundary",
    "place",
    "segment",
    "connector",
    "infrastructure",
    "land",
    "land_cover",
    "land_use",
    "water",
]

nyc_bbox = (-74.2591, 40.4766, -73.7002, 40.9174)


# took 27 s to pull NYC building data on my home wifi
def download(
    *,
    bbox: tuple[float, float, float, float] = nyc_bbox,
    output_format: Literal["geoparquet", "geojson", "geojsonseq"] = "geoparquet",
    output: Path | None = None,
    type_: OvertureType,
    release: str | None = RELEASE,
    connect_timeout: int | None = None,
    request_timeout: int | None = None,
    stac: bool = True,
):
    output = output or Path(f"{type_}.{output_format}")

    reader = record_batch_reader(
        type_, bbox, release, connect_timeout, request_timeout, stac
    )

    if reader is None:
        return

    with overturemaps_cli.get_writer(
        output_format, output, schema=reader.schema
    ) as writer:
        overturemaps_cli.copy(reader, writer)


# took 36 s to pull NYC building data on my home wifi
def download_duckdb(
    type_: OvertureType,
    output: Path,
    bbox: tuple[float, float, float, float] = nyc_bbox,
):
    import duckdb

    xmin, ymin, xmax, ymax = bbox

    duckdb.sql(f"""
        LOAD spatial; -- noqa

        SET s3_region='us-west-2';

        COPY(
        SELECT
            *
        FROM
            read_parquet('{BASE_URL}/release/{RELEASE}/theme={type_theme_map[type_]}/type={type_}/*', filename=true, hive_partitioning=1)
        WHERE
            bbox.xmin <= {xmax} AND
            bbox.xmax >= {xmin} AND
            bbox.ymin <= {ymax} AND
            bbox.ymax >= {ymin}
        ) TO '{output}';
    """)


class OvertureConnector(Connector):
    conn_type: str = "overture"

    def _pull(
        self,
        key: str,
        destination_path: Path,
        *,
        format: Literal["geoparquet", "geojson"] = "geoparquet",
        filename: str | None = None,
        bbox: tuple[float, float, float, float] = nyc_bbox,
        **kwargs,
    ) -> dict:
        extension = "parquet" if format == "geoparquet" else format
        filename = filename or f"{key}.{extension}"
        destination_path.mkdir(parents=True, exist_ok=True)
        output = destination_path / filename
        download(type_=key, bbox=bbox, output_format=format, output=output)
        # download_duckdb(type_=key, bbox=bbox, output=output)
        return {"path": output}

    def pull(self, key: str, destination_path: Path, **kwargs):
        return self._pull(key, destination_path, **kwargs)
