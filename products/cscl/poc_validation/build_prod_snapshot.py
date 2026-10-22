"""Build production_outputs.zip: one release's production outputs as parquet, for QA.

QA compares the build against the legacy pipeline's outputs. This turns those outputs
(the flat files and the two FGDBs, from edm-private/cscl_etl/<version>/ and the DCP
website) into one parquet file per table, named as the production_outputs tables the QA
models read, zipped into a single bundle. The build loads the bundle as a recipe input
(file_type parquet_bundle) into its production_outputs schema.

- Flat files are parsed with prod_data_loader's parsers.
- FGDB layers go through dcpy.geospatial.curves, so prod's curved geometry is
  linearized exactly as the build's is; columns lowercased, the geometry named
  `shape` and the FIDs kept as `objectid`, as the QA models read them.
- Prod's ETL logs (log4net, one file per ExtractorUI session), when someone has put
  them in edm-private/cscl_etl/<version>/logs/, become the `etl_log` table: one row per
  log entry. They're optional: without them `etl_log` is empty.

Usage (from products/cscl), or run the "CSCL - Prod snapshot" workflow:
    python poc_validation/build_prod_snapshot.py check-inputs -v 26d
    python poc_validation/build_prod_snapshot.py build -v 26d [--upload [--overwrite]]
    python poc_validation/build_prod_snapshot.py check-snapshot -v 26d
"""

import json
import os
import re
import shutil
import sys
import tempfile
import urllib.error
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

from dcpy import configuration  # noqa: E402
from dcpy.geospatial.curves.layer import layer_to_parquet  # noqa: E402
from dcpy.geospatial.gdb import fgdb  # noqa: E402
from dcpy.utils import s3  # noqa: E402
from dcpy.utils.logging import logger  # noqa: E402

DATASET = "dcp_cscl_production_outputs"
BUNDLE_NAME = "production_outputs.zip"
LION_GDB_URL = "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/lion/nyclion_{version}.zip"
# the legacy outputs live in edm-private/cscl_etl/ (as prod_data_loader reads them); the
# snapshot goes where the recipe input's edm.private connector looks for it
LEGACY_BUCKET = "edm-private"
SNAPSHOT_BUCKET = configuration.PRIVATE_BUCKET
WORKFLOW = "CSCL - Prod snapshot"


# log4net's layout in prod's ETL: `<date time,ms> [thread] LEVEL logger [ndc] - message`
ETL_LOG_ENTRY = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[[^\]]*\] (\w+)\s+\S+ \[[^\]]*\] - (.*)$"
)
ETL_LOG_COLUMNS = ["log_file", "line_number", "logged_at", "log_level", "message"]


def snapshot_key(version: str) -> str:
    return f"datasets/{DATASET}/{version}/{BUNDLE_NAME}"


def expected_files(version: str) -> list[str]:
    """What build() reads from edm-private/cscl_etl/<version>/: the flat files QA
    compares, the LDF, and the district FGDB."""
    flat = {d.file_name for d in pdl.datasets_by_name.values() if d.compare_file}
    return sorted(flat | {"LDF.dat", "LDF.header", pdl._district_gdb_filename(version)})


def found_files(version: str) -> set[str]:
    """Files directly under edm-private/cscl_etl/<version>/ (not inside a .gdb folder)."""
    prefix = f"cscl_etl/{version}/"
    keys = [o["Key"] for o in s3.list_objects(LEGACY_BUCKET, prefix)]
    return {k[len(prefix) :] for k in keys if "/" not in k[len(prefix) :]} - {""}


def url_exists(url: str) -> bool:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="HEAD")):
            return True
    except urllib.error.HTTPError:
        return False


def missing_inputs(version: str) -> list[str]:
    """Inputs build() needs for this release that aren't there yet, logging expected
    vs found."""
    expected = expected_files(version)
    found = found_files(version)
    missing = [f"cscl_etl/{version}/{f}" for f in expected if f not in found]
    lion_url = LION_GDB_URL.format(version=version)
    if not url_exists(lion_url):
        missing.append(lion_url)
    if not found:
        logger.warning(f"edm-private/cscl_etl/{version}/ is missing or empty")
    logger.info(
        f"edm-private/cscl_etl/{version}/: expected {len(expected)} files, "
        f"found {len(set(expected) & found)} of them ({len(found)} files in all)"
    )
    for f in expected:
        logger.info(f"  {'found  ' if f in found else 'MISSING'} {f}")
    logger.info(f"  {'MISSING' if lion_url in missing else 'found  '} {lion_url}")
    return missing


def etl_logs_prefix(version: str) -> str:
    return f"cscl_etl/{version}/logs/"


def parse_etl_log(text: str, log_file: str) -> list[dict]:
    """One row per log4net entry. A line that doesn't start an entry (a message with a
    line break in it) is appended to the entry before it."""
    rows: list[dict] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        match = ETL_LOG_ENTRY.match(line)
        if match:
            logged_at, level, message = match.groups()
            rows.append(
                {
                    "log_file": log_file,
                    "line_number": line_number,
                    "logged_at": logged_at,
                    "log_level": level.lower(),
                    "message": message.rstrip(),
                }
            )
        elif rows and line.strip():
            rows[-1]["message"] += "\n" + line.rstrip()
        elif line.strip():
            raise ValueError(
                f"{log_file}:{line_number} isn't a log4net entry: {line!r}"
            )
    return rows


def etl_log_frame(log_files: list[Path]) -> pd.DataFrame:
    """Prod's ETL log entries, all files together (empty, with the columns, if none)."""
    rows = [
        row
        for f in sorted(log_files)
        for row in parse_etl_log(f.read_text(encoding="utf-8-sig"), f.name)
    ]
    frame = pd.DataFrame(rows, columns=ETL_LOG_COLUMNS)
    return frame.astype(
        {"line_number": "int64"}
        | {c: "string" for c in ETL_LOG_COLUMNS if c != "line_number"}
    )


def download_etl_logs(version: str, folder: Path) -> list[Path]:
    """Prod's ETL logs for this release from edm-private, if anyone has put them there."""
    prefix = etl_logs_prefix(version)
    keys = [
        o["Key"]
        for o in s3.list_objects(LEGACY_BUCKET, prefix)
        if o["Key"].lower().endswith(".log")
    ]
    if not keys:
        logger.warning(
            f"No ETL logs in {LEGACY_BUCKET}/{prefix}: etl_log will be empty, and "
            "qa__log_compare has nothing from prod to compare"
        )
    folder.mkdir(parents=True, exist_ok=True)
    paths = []
    for key in keys:
        path = folder / Path(key).name
        s3.download_file(LEGACY_BUCKET, key, path)
        paths.append(path)
    return paths


class ParquetSink:
    """prod_data_loader's sink: tables become parquet files."""

    schema = "production_outputs"

    def __init__(self, folder: Path):
        self.folder = folder
        self.frames: dict[str, pd.DataFrame] = {}

    def insert_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        df = df.rename(columns=str)
        if df.columns.empty:
            # an empty prod file parses to no columns (parquet needs at least one): take
            # them from the file's layout
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
    """Replace the 1..N ogc_fid with the layer's real FIDs (OBJECTID). Both are in FID
    order."""
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
        # geometry as `shape`
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


def build(version: str, out_dir: Path, logs_dir: Path | None = None) -> Path:
    tables = out_dir / "tables"
    files = out_dir / "files"
    for d in (tables, files):
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True)

    sink = ParquetSink(tables)

    # flat files (downloaded, parsed with their layouts) and the citywide tables
    loaded = pdl.load_flat_files(sink, version, files)
    if any(dataset.endswith("_face_code") for dataset in loaded):
        boroughs = ["bronx", "brooklyn", "manhattan", "queens", "staten_island"]
        sink.insert_dataframe(
            pd.concat(
                [sink.frames[f"{b}_face_code"] for b in boroughs], ignore_index=True
            ),
            "citywide_face_code",
        )
    pdl.load_citywide_lion(sink, version, "citywide_lion_dat", files)
    pdl.load_prod_ldf(sink, version, files)

    # prod's ETL logs (optional)
    log_files = (
        sorted(logs_dir.glob("*.log"))
        if logs_dir
        else download_etl_logs(version, files / "logs")
    )
    sink.insert_dataframe(etl_log_frame(log_files), "etl_log")

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


@app.command("check-inputs")
def _check_inputs(version: str = typer.Option(pdl.version, "--version", "-v")):
    """Fail unless every legacy output build() reads for this release exists."""
    missing = missing_inputs(version)
    if missing:
        logger.error(
            f"{version}: {len(missing)} of the legacy outputs the snapshot needs are "
            "missing (MISSING above); the legacy release isn't complete yet"
        )
        raise typer.Exit(1)


@app.command("check-snapshot")
def _check_snapshot(version: str = typer.Option(pdl.version, "--version", "-v")):
    """Fail unless the snapshot for this release has been uploaded."""
    key = snapshot_key(version)
    if not s3.object_exists(SNAPSHOT_BUCKET, key):
        logger.error(
            f"No prod snapshot for {version} (s3://{SNAPSHOT_BUCKET}/{key}): QA compares "
            f'the build against it. Run the "{WORKFLOW}" workflow with version {version} '
            f"(or poc_validation/build_prod_snapshot.py build -v {version} --upload)."
        )
        raise typer.Exit(1)
    logger.info(f"Prod snapshot for {version}: s3://{SNAPSHOT_BUCKET}/{key}")


@app.command("build")
def _build(
    version: str = typer.Option(pdl.version, "--version", "-v"),
    out_dir: Path = typer.Option(Path(".data/prod_snapshot"), "--out", "-o"),
    upload: bool = typer.Option(
        False, "--upload", help=f"Upload to edm-private/datasets/{DATASET}/<version>/"
    ),
    overwrite: bool = typer.Option(
        False, "--overwrite", help="With --upload: replace an existing snapshot"
    ),
    logs_dir: Path | None = typer.Option(
        None,
        "--logs-dir",
        help="Read prod's ETL logs (*.log) from this folder instead of "
        "edm-private/cscl_etl/<version>/logs/",
    ),
):
    key = snapshot_key(version)
    if upload and not overwrite and s3.object_exists(SNAPSHOT_BUCKET, key):
        logger.error(
            f"s3://{SNAPSHOT_BUCKET}/{key} exists; pass --overwrite to replace it"
        )
        raise typer.Exit(1)
    _check_inputs(version)
    bundle = build(version, out_dir, logs_dir)
    if upload:
        s3.upload_file(SNAPSHOT_BUCKET, bundle, key, "private")
        logger.info(f"Uploaded to s3://{SNAPSHOT_BUCKET}/{key}")


if __name__ == "__main__":
    app()
