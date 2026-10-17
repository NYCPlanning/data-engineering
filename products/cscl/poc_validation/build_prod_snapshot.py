"""Build production_outputs.zip: one release's production outputs as parquet, for QA.

QA compares the build against the legacy pipeline's outputs. This turns those outputs
(the flat files and the two FGDBs, from edm-private/cscl_etl/<version>/ and the DCP
website) into one parquet file per table, named as the production_outputs tables the QA
models read, zipped into a single bundle. The build loads the bundle as a recipe input
(file_type parquet_bundle) into its production_outputs schema.

- Flat files are parsed exactly as prod_data_loader always has (its parsers, with a
  parquet sink in place of Postgres).
- FGDB layers go through dcpy.geospatial.curves, so prod's curved geometry is
  linearized exactly as the build's is; columns lowercased and the geometry named
  `shape`, as the ogr2ogr loads into Postgres had them.

Usage (from products/cscl):
    python poc_validation/build_prod_snapshot.py -v 26c [--upload]
"""

import json
import os
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import typer
from pyogrio.raw import read_arrow

# prod_data_loader holds the prod file parsers; it reads the recipe and settings on
# import, including where prod files go (overridden per build below)
os.environ.setdefault(
    "PROD_FILES_DIR", str(Path(".data/prod_snapshot/files").absolute())
)
sys.path.insert(0, str(Path(__file__).parent))
import prod_data_loader as pdl  # noqa: E402

from dcpy.geospatial.curves.layer import layer_to_parquet  # noqa: E402
from dcpy.geospatial.gdb import fgdb  # noqa: E402
from dcpy.utils import s3  # noqa: E402
from dcpy.utils.logging import logger  # noqa: E402

DATASET = "dcp_cscl_production_outputs"
BUNDLE_NAME = "production_outputs.zip"
LION_GDB_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/lion/nyclion_{version}.zip"


class ParquetSink:
    """Stands in for prod_data_loader's Postgres client: tables become parquet files."""

    schema = "production_outputs"

    def __init__(self, folder: Path):
        self.folder = folder
        self.frames: dict[str, pd.DataFrame] = {}

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        df = df.rename(columns=str)
        if df.columns.empty:
            # an empty prod file parses to no columns (parquet needs at least one): take
            # them from the file's layout, as the Postgres table had them
            dataset = pdl.datasets_by_name.get(table_name)
            layout = dataset.formatting_path if dataset else None
            names = (
                list(pd.read_csv(layout)["field_name"]) if layout else ["dat_column"]
            )
            df = pd.DataFrame({n: pd.Series(dtype="string") for n in names})
        self.frames[table_name] = df
        df.to_parquet(self.folder / f"{table_name}.parquet", index=False)
        logger.info(f"{table_name}: {len(df)} rows")


def _lowercase(path: Path) -> None:
    table = pq.read_table(path)
    metadata = table.schema.metadata
    table = table.rename_columns([c.lower() for c in table.column_names])
    pq.write_table(table.replace_schema_metadata(metadata), path)


def _objectid(gdb: Path, layer: str, table: pa.Table) -> pa.Table:
    """Replace the 1..N ogc_fid with the layer's real FIDs (OBJECTID), as the Postgres
    loads kept them. Both are in FID order."""
    _, fids = read_arrow(
        str(gdb), layer=layer, read_geometry=False, columns=[], return_fids=True
    )
    objectid = fids.column(0).cast(pa.int64())
    assert len(objectid) == table.num_rows, f"{layer}: fid count mismatch"
    i = table.column_names.index("ogc_fid")
    return table.set_column(i, "objectid", objectid)


def _write_gdb_layer(gdb: Path, layer: str, out: Path) -> None:
    """One FGDB layer as parquet: linearized by dcpy.geospatial.curves, geometry as
    `shape`, columns lowercased (an attribute table has no geometry column)."""
    info = fgdb.layer_geometry_types(gdb)
    if info.get(layer) is None:
        _, table = read_arrow(str(gdb), layer=layer, read_geometry=False)
        for i, field in enumerate(table.schema):  # coded domains: keep the codes
            if pa.types.is_dictionary(field.type):
                codes = pa.chunked_array(
                    [c.indices for c in table.column(i).chunks],  # type: ignore[attr-defined]
                    type=field.type.index_type,
                )
                table = table.set_column(i, field.name, codes)
        table = table.add_column(
            0, "ogc_fid", pa.array(range(1, table.num_rows + 1), pa.int32())
        )
        pq.write_table(_objectid(gdb, layer, table), out)
    else:
        with tempfile.TemporaryDirectory() as tmp:
            n, _ = layer_to_parquet(gdb, layer, out, Path(tmp) / "arcs.parquet")
        if n == 0:
            # no GeoParquet is written for an empty layer: write its (empty) attributes
            # with a WKB `shape`, flagged as GeoParquet so DuckDB types it GEOMETRY
            logger.warning(f"{layer}: no features")
            _, attrs = read_arrow(str(gdb), layer=layer, read_geometry=False)
            attrs = attrs.add_column(0, "objectid", pa.array([], pa.int64()))
            attrs = attrs.append_column("shape", pa.array([], pa.binary()))
            geo = {
                "version": "1.1.0",
                "primary_column": "shape",
                "columns": {"shape": {"encoding": "WKB", "geometry_types": []}},
            }
            pq.write_table(
                attrs.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), out
            )
            _lowercase(out)
            return
        geo = json.loads(pq.ParquetFile(out).metadata.metadata[b"geo"])
        geom = geo["primary_column"]
        table = _objectid(gdb, layer, pq.read_table(out))
        # geometry as `shape`, as the ogr2ogr loads into Postgres named it
        geo["columns"]["shape"] = geo["columns"].pop(geom)
        geo["primary_column"] = "shape"
        table = table.rename_columns(
            ["shape" if c == geom else c for c in table.column_names]
        ).replace_schema_metadata({b"geo": json.dumps(geo).encode()})
        pq.write_table(table, out)
    _lowercase(out)


def _extracted_gdb(zip_path: Path) -> Path:
    """GDAL reads a GDB inside a zip far slower than an extracted one: extract first."""
    out = zip_path.with_name(f"{zip_path.name}.unzipped")
    shutil.rmtree(out, ignore_errors=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out)
    gdbs = [p for p in out.rglob("*.gdb") if p.is_dir() and "__MACOSX" not in p.parts]
    assert len(gdbs) == 1, f"expected one .gdb in {zip_path}, found {gdbs}"
    return gdbs[0]


def _gdb_layers(gdb_zip: Path, skip: set[str], folder: Path) -> None:
    gdb = _extracted_gdb(gdb_zip)
    for layer in fgdb.get_layers(gdb):
        if layer in skip:
            logger.info(f"{layer}: frozen in prod, skipped")
            continue
        _write_gdb_layer(gdb, layer, folder / f"fgdb_{layer.lower()}.parquet")
        logger.info(f"fgdb_{layer.lower()} written")


def build(version: str, out_dir: Path) -> Path:
    tables = out_dir / "tables"
    files = out_dir / "files"
    for d in (tables, files):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)

    sink = ParquetSink(tables)
    pdl.CLIENT = sink
    pdl.already_loaded = lambda *args, **kwargs: False
    pdl.record_load = lambda *args, **kwargs: None

    def citywide(file: str) -> None:
        boroughs = ["bronx", "brooklyn", "manhattan", "queens", "staten_island"]
        sink.insert_dataframe(
            pd.concat(
                [sink.frames[f"{b}_{file}"] for b in boroughs], ignore_index=True
            ),
            f"citywide_{file}",
        )

    pdl.create_citywide_table = citywide

    # flat files (downloaded, parsed with their layouts) and the citywide tables
    pdl._load(
        datasets=list(pdl.datasets_by_name),
        version=version,
        local=False,
        local_folder=files,
        force=True,
    )
    pdl.load_citywide_lion(version, "citywide_lion_dat", files, force=True)
    pdl._load_prod_ldf(version=version, local_folder=files, force=True)

    # the two FGDBs
    lion_zip = files / f"nyclion_{version}.zip"
    urllib.request.urlretrieve(LION_GDB_URL.format(version=version), lion_zip)
    _gdb_layers(lion_zip, set(), tables)
    districts = pdl._district_gdb_filename(version)
    s3.download_file(
        "edm-private", f"cscl_etl/{version}/{districts}", files / districts
    )
    _gdb_layers(files / districts, pdl._frozen_district_layers(), tables)

    bundle = out_dir / BUNDLE_NAME
    bundle.unlink(missing_ok=True)
    with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as zf:
        for parquet in sorted(tables.glob("*.parquet")):
            zf.write(parquet, parquet.name)
    logger.info(f"{bundle}: {len(list(tables.glob('*.parquet')))} tables")
    return bundle


app = typer.Typer(add_completion=False)


@app.command("build")
def _build(
    version: str = typer.Option(pdl.version, "--version", "-v"),
    out_dir: Path = typer.Option(Path(".data/prod_snapshot"), "--out", "-o"),
    upload: bool = typer.Option(
        False, "--upload", help=f"Upload to edm-private/datasets/{DATASET}/<version>/"
    ),
):
    bundle = build(version, out_dir)
    if upload:
        key = f"datasets/{DATASET}/{version}/{BUNDLE_NAME}"
        s3.upload_file("edm-private", bundle, key, "private")
        logger.info(f"Uploaded to s3://edm-private/{key}")


if __name__ == "__main__":
    app()
