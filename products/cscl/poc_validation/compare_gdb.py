"""
Compare dev and prod GDB layers for structure and data parity - covers every
gdb-format export in recipe.yml (LION gdb, district gdb, any future ones).

Run from the products/cscl directory:
    python poc_validation/compare_gdb.py

The GDB filename(s) and version are resolved from recipe.yml. By default:
    dev:  output/dataset_files/<gdb filename>        (from the local/CI build)
    prod: edm-private/cscl_etl/<version>/<filename>  (fetched to .data/prod/)

For local iteration without S3 access, pass a local prod copy with --prod
(e.g. --prod ../../.task-pipeline/nyclion_26a.zip).

Each layer's row identity for the keyed row-level diff comes from
seeds/lion_outputs.csv's key_columns column, not from guessing at comparison
time - see _load_declared_keys and that column's doc in seeds.yml for why.
Add an entry there (verified against a real build) for any layer that logs a
"no declared key" warning.

By default, NULL and whitespace-only strings compare as equal (prod's FileGDB
export stores "no value" as blank/spaces for many text fields where we store
a real NULL - see dcpy.geospatial.gdb.compare.stringify's docstring). Whether
we should adopt that convention ourselves is undecided, so this is a default,
not a fact - pass --strict-nulls to compare them as distinct instead. This
only affects attribute values, never row identity: a layer's key columns are
always compared exactly (see dcpy.geospatial.gdb.compare.composite_key).

Only geometry-derived measures (Shape_Length/Shape_Area, matched
case-insensitively - see
dcpy.geospatial.gdb.compare.DEFAULT_GEOMETRY_DERIVED_FLOAT_COLUMNS) get the
recompute-noise float tolerance - every other float column, including
ordinary numeric attributes, is compared exactly.

The comparison logic and its report formatting both live in
dcpy.geospatial.gdb (compare.py / report.py) - this script only supplies
CSCL's own inputs (declared keys, known/expected diffs) and CLI/S3 wiring.

Report-only: writes a per-column CSV per gdb to output/validation_output/<name>_comparison.csv
and logs a report. It never fails the build on a data mismatch.
"""

import csv
from pathlib import Path

import geopandas as gpd
import typer

from dcpy.geospatial.gdb import compare as gdb_compare
from dcpy.geospatial.gdb import fgdb
from dcpy.geospatial.gdb import report as gdb_report
from dcpy.lifecycle.builds import plan
from dcpy.utils import s3
from dcpy.utils.logging import logger

OUTPUT_DIR = Path("output/validation_output")
LION_OUTPUTS_SEED_PATH = Path("seeds/lion_outputs.csv")
PROD_BUCKET = "edm-private"

# Layers with a known, already-understood structural diff - the layer note gets a
# "KNOWN: " prefix instead of reading like a fresh problem every run.
KNOWN_STRUCTURAL_DIFFS = {
    # Prod's nyura carries a stale copy of nybid's schema; both are empty, so the
    # structural diff on this layer is expected. See models/product/districts/gdb_nyura.sql.
    # Root cause confirmed via FileGDB item metadata: nyura and nybid share the same frozen
    # 2009-10-20 CopyFeatures batch (Windows XP/ArcGIS 9.3 era) - see
    # docs/prod_bugs/013-gdb-creadate-staleness-fossils.md.
    "nyura",
}

# Columns hardcoded to a NULL placeholder in a gdb_<layer>.sql model (unimplemented
# fields, not bugs) - excluded from a layer's flagged-column count/list so it reflects
# genuinely unexplained gaps, not fields already known to be unimplemented. Still shown
# per-column in the CSV with a "KNOWN:" note rather than silently dropped.
KNOWN_NULL_COLUMNS: dict[str, set[str]] = {
    # Fields still hardcoded to a NULL placeholder in gdb_lion.sql, and why:
    # - SplitSchl: per ETL spec, an unused one-digit filler in Geosupport LION - blank
    #   in prod 100% of the time (confirmed against production_outputs.fgdb_lion), so
    #   this isn't a gap, just a field with no real content to derive.
    # - Radius: tied to the ArcCenterX/Y curve-geometry issue (CSCL-LION-07,
    #   data_issues.md) - on hold, not implemented.
    # - FromLeft/ToLeft/FromRight/ToRight: real prod data contradicts a literal reading
    #   of the spec's zero-out rule - needs dedicated investigation (see gdb_lion.sql).
    # See models/product/lion/gdb/gdb_lion.sql for the NULL::text/NULL::int literals.
    "lion": {
        "SplitSchl",
        "Radius",
        "FromLeft",
        "ToLeft",
        "FromRight",
        "ToRight",
    },
}

app = typer.Typer(add_completion=False)


def _load_declared_keys(seed_path: Path) -> dict[str, list[str]]:
    """layer name -> declared key columns, from lion_outputs.csv's key_columns
    column (gdb rows only, pipe-separated).

    Keys are declared, not inferred at comparison time - see that column's doc
    in seeds.yml for why: a column that happens to be unique in today's data
    (e.g. SHAPE_Length) isn't a real identity, and auto-detection would pick a
    different key on a different run with no warning. Add a row here (and
    verify it against a real build) for any layer this doesn't cover yet -
    dcpy.geospatial.gdb.compare.guess_key_columns is only a stopgap for that gap,
    not a substitute.
    """
    if not seed_path.exists():
        return {}
    declared: dict[str, list[str]] = {}
    with seed_path.open(newline="") as f:
        for row in csv.DictReader(f):
            if row.get("type") != "layer" or not row.get("key_columns"):
                continue
            declared[row["filename"]] = row["key_columns"].split("|")
    return declared


def _compare_layers(
    dev_path: Path,
    prod_path: Path,
    report_name: str,
    polygon_layers: set[str],
    declared_keys: dict[str, list[str]],
    blank_as_null: bool = True,
) -> None:
    report = gdb_report.GdbComparisonReport()
    report.log_settings(blank_as_null)

    # fgdb.layer_geometry_types/gpd.read_file both resolve a zipped GDB
    # natively (no manual /vsizip/ path needed).
    report.log_layer_structure(
        fgdb.layer_geometry_types(dev_path), fgdb.layer_geometry_types(prod_path)
    )

    for layer in report.common_layers:
        dev_gdf = gpd.read_file(dev_path, layer=layer)
        prod_gdf = gpd.read_file(prod_path, layer=layer)

        # Match columns case-insensitively before comparing - FileGDB/ArcGIS
        # export tooling isn't consistent about the casing of its own built-in
        # fields (Shape_Area vs SHAPE_Area has shown up across releases), and
        # compare_layer's exact-name structure_diff would otherwise drop a
        # column from every row/column-level comparison whenever the two
        # sides disagree on case, silently hiding a real content difference
        # behind what reads like a missing/extra-column note. Harmonize
        # prod's spelling to dev's for every case-insensitive match so
        # compare_layer (which assumes identical column names) never sees
        # the mismatch.
        col_match = gdb_compare.match_columns(dev_gdf.columns, prod_gdf.columns)
        if col_match.case_mismatches:
            prod_gdf = prod_gdf.rename(
                columns=dict((p, d) for d, p in col_match.case_mismatches)
            )

        result = gdb_compare.compare_layer(
            dev_gdf,
            prod_gdf,
            declared_key=declared_keys.get(layer),
            is_polygon=layer in polygon_layers,
            blank_as_null=blank_as_null,
        )

        if result.declared_key_rejected:
            logger.warning(
                f"declared key {result.declared_key_rejected} for layer {layer} "
                "isn't in this build's columns - guessing instead. Update "
                "lion_outputs.csv's key_columns."
            )
        elif result.key_was_guessed:
            logger.warning(
                f"no declared key for layer {layer} in lion_outputs.csv - guessing "
                "one for this run only. Add a key_columns entry once you've "
                "verified one against real data."
            )

        # KNOWN_NULL_COLUMNS is CSCL's own annotation, not something
        # compare_layer/report can know about - overlaid here on its generic
        # stats before the report ever sees them.
        for s in result.column_stats:
            if s.all_null_in_dev and s.column in KNOWN_NULL_COLUMNS.get(layer, set()):
                s.note = "KNOWN: unimplemented (hardcoded null)"

        report.add_layer(
            layer,
            result,
            len(dev_gdf),
            len(prod_gdf),
            known=layer in KNOWN_STRUCTURAL_DIFFS,
        )

    report.log_summary()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"{report_name}_comparison.csv"
    rows = report.rows()
    if rows:
        with out_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Per-column CSV written to {out_csv}")


@app.command()
def run(
    recipe_path: Path = typer.Option(
        Path("recipe.yml"), "--recipe", "-r", help="Recipe to resolve gdb exports from"
    ),
    prod_version: str | None = typer.Option(
        None,
        "--prod-version",
        "-v",
        help="Prod CSCL version under edm-private/cscl_etl/. Defaults to recipe version.",
    ),
    dev: Path | None = typer.Option(
        None, "--dev", "-d", help="Dev GDB zip. Default: output/<gdb filename>."
    ),
    prod: Path | None = typer.Option(
        None,
        "--prod",
        "-p",
        help="Local prod GDB zip. If omitted, fetched from S3 instead.",
    ),
    treat_blank_as_null: bool = typer.Option(
        True,
        "--treat-blank-as-null/--strict-nulls",
        help=(
            "Treat NULL and whitespace-only strings as equivalent (default). "
            "Prod's FileGDB export stores 'no value' as blank/spaces where we "
            "store a real NULL - it's undecided whether we should adopt that "
            "convention, so pass --strict-nulls to compare them as distinct "
            "and see whether it would actually change anything."
        ),
    ),
) -> None:
    """Compare dev vs prod GDB layers for each gdb export in the recipe."""
    recipe = plan.recipe_from_yaml(recipe_path)
    assert recipe.exports, "recipe has no exports"
    gdb_exports = [e for e in recipe.exports.datasets if e.format.value == "gdb"]
    if not gdb_exports:
        logger.info("No gdb-format exports in recipe; nothing to compare.")
        return

    gdb_filenames = sorted({e.filename for e in gdb_exports if e.filename})
    polygon_layers: set[str] = set()
    for export in gdb_exports:
        custom = export.custom or {}
        layer = custom.get("layer")
        if custom.get("geometry_type") == "polygons" and layer:
            polygon_layers.add(layer)

    declared_keys = _load_declared_keys(LION_OUTPUTS_SEED_PATH)

    version = prod_version or recipe.version
    for filename in gdb_filenames:
        dev_path = (
            dev if dev is not None else Path("output") / "dataset_files" / filename
        )
        if prod is not None:
            prod_path = prod
        else:
            prod_path = Path(".data/prod") / filename
            key = f"cscl_etl/{version}/{filename}"
            logger.info(f"Fetching prod GDB s3://{PROD_BUCKET}/{key}")
            s3.download_file(PROD_BUCKET, key, prod_path)

        logger.info(f"dev:  {dev_path.resolve()}")
        logger.info(f"prod: {prod_path.resolve()}")
        _compare_layers(
            dev_path,
            prod_path,
            Path(filename).name.split(".")[0],
            polygon_layers,
            declared_keys,
            treat_blank_as_null,
        )


if __name__ == "__main__":
    app()
