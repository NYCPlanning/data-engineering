"""
Write the district review FGDB: every district layer next to the previous release's
(production_outputs, loaded from prod's parquet bundle), for review by another team.

Run from the products/cscl directory, after the qa__district_review_* models (part of
the QA stage's qa_models step):
    python poc_validation/district_review_gdb.py

Writes <QA_DIR>/validation_output/district_review.gdb.zip. Per district layer <L>:
    <L>_new    our districts, with the discrepancy metrics and class
               (see models/etl_dev_qa/district_review/qa__district_review_districts.sql)
    <L>_prior  prod's districts, same keys and fields
    <L>_diff   symmetric-difference pieces above grid noise: side (added | removed),
               area, district key
plus two attribute tables: summary (every non-identical district, ranked) and
summary_by_layer (class counts per layer).

Each layer's key field is named after its declared key (seeds/lion_outputs.csv); a
composite key's columns are joined with '_' and its values with '|'.
"""

import json
import os
import tempfile
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import typer
from paths import validation_dir

from dcpy.geospatial.gdb.write import ParquetLayer, write_gdb_from_parquet
from dcpy.utils.logging import logger

CRS = "EPSG:2263"
METRIC_COLUMNS = [
    "area_new",
    "area_prior",
    "area_delta",
    "area_delta_pct",
    "symdiff_area",
    "symdiff_area_pct",
    "hausdorff_ft",
    "symdiff_boundary_len_ft",
    "symdiff_fragment_count",
    "class",
    "known_difference_id",
]

app = typer.Typer(add_completion=False)


def _write_parquet(
    conn: duckdb.DuckDBPyConnection, query: str, params: list, path: Path
) -> int:
    """Write a query's result to ``path``; a ``geom`` WKB column gets GeoParquet
    metadata (MultiPolygon, EPSG:2263) so it's written as a feature class."""
    table = conn.execute(query, params).fetch_arrow_table()
    if "geom" in table.column_names:
        geo = {
            "version": "1.0.0",
            "primary_column": "geom",
            "columns": {
                "geom": {
                    "encoding": "WKB",
                    "geometry_types": ["MultiPolygon"],
                    "crs": CRS,
                }
            },
        }
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)
    return table.num_rows


def _layer_queries(key_name: str) -> dict[str, str]:
    metrics = ", ".join(METRIC_COLUMNS)
    return {
        "new": f"""
            SELECT district_key AS "{key_name}", {metrics},
                st_aswkb(st_multi(geom_new)) AS geom
            FROM qa__district_review_districts
            WHERE layer = ? AND geom_new IS NOT NULL
            ORDER BY district_key""",
        "prior": f"""
            SELECT district_key AS "{key_name}", {metrics},
                st_aswkb(st_multi(geom_prior)) AS geom
            FROM qa__district_review_districts
            WHERE layer = ? AND geom_prior IS NOT NULL
            ORDER BY district_key""",
        "diff": f"""
            SELECT district_key AS "{key_name}", side, area_sqft AS area,
                mean_width_ft, st_aswkb(st_multi(geom)) AS geom
            FROM qa__district_review_pieces
            WHERE layer = ? AND NOT is_noise
            ORDER BY district_key, area_sqft DESC""",
    }


@app.command()
def run(
    output: Path = typer.Option(
        None, "--output", "-o", help="Default: <validation dir>/district_review.gdb.zip"
    ),
) -> None:
    output = output or validation_dir() / "district_review.gdb.zip"
    conn = duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True)
    conn.execute("LOAD spatial")
    conn.execute(f'SET schema = \'{os.environ["BUILD_ENGINE_SCHEMA"]}\'')

    layers = conn.execute(
        "SELECT layer, any_value(key_name) FROM qa__district_review_districts "
        "GROUP BY layer ORDER BY layer"
    ).fetchall()

    with tempfile.TemporaryDirectory() as tmp:
        parquet_layers: list[ParquetLayer] = []
        for layer, key_name in layers:
            for suffix, query in _layer_queries(key_name).items():
                path = Path(tmp) / f"{layer}_{suffix}.parquet"
                n = _write_parquet(conn, query, [layer], path)
                logger.info(f"{layer}_{suffix}: {n:,} features")
                parquet_layers.append(
                    ParquetLayer(f"{layer}_{suffix}", path, geometry_type="MultiPolygon")
                )
        for name, query in {
            "summary": "SELECT * FROM qa__district_review_summary ORDER BY rank",
            "summary_by_layer": "SELECT * FROM qa__district_review_layer_summary ORDER BY layer",
        }.items():
            path = Path(tmp) / f"{name}.parquet"
            _write_parquet(conn, query, [], path)
            parquet_layers.append(ParquetLayer(name, path))

        write_gdb_from_parquet(parquet_layers, output)
    logger.info(f"{output}: {output.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    app()
