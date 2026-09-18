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
a real NULL - see dcpy.geospatial.compare.stringify's docstring). Whether we
should adopt that convention ourselves is undecided, so this is a default,
not a fact - pass --strict-nulls to compare them as distinct instead.

Report-only: writes a per-column CSV per gdb to output/validation_output/<name>_comparison.csv
and prints a report to stdout. It never fails the build on a data mismatch.
"""

import csv
import zipfile
from pathlib import Path

import geopandas as gpd
import pyogrio
import typer

from dcpy.geospatial.gdb import compare as gdb_compare
from dcpy.lifecycle.builds import plan
from dcpy.utils import s3

OUTPUT_DIR = Path("output/validation_output")
LION_OUTPUTS_SEED_PATH = Path("seeds/lion_outputs.csv")
PROD_BUCKET = "edm-private"

# Area comparison only makes sense for polygon layers - a line/point layer's area is
# always zero. AREA_PCT_THRESHOLD matches the tolerance compare_districts.py used.
AREA_PCT_THRESHOLD = 0.5

# Above this many flagged columns, name a count instead of listing every column -
# a layer with dozens of null-rate anomalies would otherwise produce an unreadable
# one-line note.
FLAGGED_COLUMNS_LISTED = 5

# Layers with a known, already-understood structural diff - the layer note gets a
# "KNOWN: " prefix instead of reading like a fresh problem every run.
KNOWN_STRUCTURAL_DIFFS = {
    # Prod's nyura carries a stale copy of nybid's schema; both are empty, so the
    # structural diff on this layer is expected. See models/product/districts/gdb_nyura.sql.
    "nyura",
}

# Columns hardcoded to a NULL placeholder in a gdb_<layer>.sql model (unimplemented
# fields, not bugs) - excluded from a layer's flagged-column count/list so it reflects
# genuinely unexplained gaps, not fields already known to be unimplemented. Still shown
# per-column in the CSV with a "KNOWN:" note rather than silently dropped.
KNOWN_NULL_COLUMNS: dict[str, set[str]] = {
    # 21 fields with no source in our current pipeline (roadbed/SAF-scope, mostly) -
    # see the NULL::text/NULL::int literals in models/product/lion/gdb/gdb_lion.sql.
    "lion": {
        "Street",
        "SAFStreetName",
        "RB_Layer",
        "TrafSrc",
        "SAFStreetCode",
        "RBoro",
        "L_CD",
        "R_CD",
        "LCT1990",
        "LCT1990Suf",
        "RCT1990",
        "RCT1990Suf",
        "SplitSchl",
        "MH_RI_Flag",
        "Radius",
        "ACTIVE_FLAG",
        "Carto_Display_Level",
        "FromLeft",
        "ToLeft",
        "FromRight",
        "ToRight",
    },
}

app = typer.Typer(add_completion=False)


def _inner_gdb(zip_path: Path) -> str:
    """Return the /vsizip/... path to the .gdb inside a zip."""
    abs_path = str(zip_path.resolve())
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
    gdbs: set[str] = set()
    for n in names:
        parts = n.split("/")
        for i, p in enumerate(parts):
            if p.endswith(".gdb"):
                gdbs.add("/".join(parts[: i + 1]))
                break
    if not gdbs:
        raise ValueError(f"No .gdb found inside {zip_path}")
    gdb = next(iter(gdbs))
    return f"/vsizip/{abs_path}/{gdb}"


def _list_layers(gdb_vsi: str) -> dict[str, str | None]:
    rows = pyogrio.list_layers(gdb_vsi)
    return {str(row[0]): (str(row[1]) if row[1] else None) for row in rows}


def _load_declared_keys(seed_path: Path) -> dict[str, list[str]]:
    """layer name -> declared key columns, from lion_outputs.csv's key_columns
    column (gdb rows only, pipe-separated).

    Keys are declared, not inferred at comparison time - see that column's doc
    in seeds.yml for why: a column that happens to be unique in today's data
    (e.g. SHAPE_Length) isn't a real identity, and auto-detection would pick a
    different key on a different run with no warning. Add a row here (and
    verify it against a real build) for any layer this doesn't cover yet -
    dcpy.geospatial.compare.guess_key_columns is only a stopgap for that gap,
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


def _layer_note(
    *,
    layer: str,
    missing_from_dev: list[str],
    extra_in_dev: list[str],
    columns_match_but_order_differs: bool,
    row_level: gdb_compare.RowLevelDiff,
    area_pct_diff: float | None,
    flagged_columns: list[str],
) -> str:
    """One consolidated, human-readable note per layer - structure, keyed
    row-level diff, (for polygons) area, and any per-column null-rate/nunique
    anomaly, combined - rather than needing to scan every column's note to tell
    whether a layer is actually fine. flagged_columns is every non-spatial
    column whose own note (null rate diff, ALL NULL in dev, ...) is non-empty."""
    flags = []
    if missing_from_dev:
        flags.append(f"missing {missing_from_dev}")
    if extra_in_dev:
        flags.append(f"extra {extra_in_dev}")
    if columns_match_but_order_differs:
        flags.append("column ORDER differs")

    total_diff = (
        row_level.only_in_dev + row_level.only_in_prod + (row_level.modified or 0)
    )
    if total_diff:
        if row_level.precise:
            flags.append(
                f"{total_diff:,} rows differ ({row_level.modified:,} modified, "
                f"{row_level.only_in_dev:,} dev-only, "
                f"{row_level.only_in_prod:,} prod-only)"
            )
        else:
            flags.append(
                f"{total_diff:,} rows differ ({row_level.only_in_dev:,} dev-only, "
                f"{row_level.only_in_prod:,} prod-only)"
            )

    if area_pct_diff is not None and abs(area_pct_diff) > AREA_PCT_THRESHOLD:
        flags.append(f"area {area_pct_diff:+.2f}%")
    if len(flagged_columns) > FLAGGED_COLUMNS_LISTED:
        flags.append(f"{len(flagged_columns)} columns flagged (see per-column CSV)")
    elif flagged_columns:
        flags.append(f"columns {flagged_columns}")
    note = "; ".join(flags) or "OK"
    if flags and layer in KNOWN_STRUCTURAL_DIFFS:
        note = f"KNOWN: {note}"
    return note


def _compare_layers(
    dev_path: Path,
    prod_path: Path,
    report_name: str,
    polygon_layers: set[str],
    declared_keys: dict[str, list[str]],
    blank_as_null: bool = True,
) -> None:
    dev_gdb = _inner_gdb(dev_path)
    prod_gdb = _inner_gdb(prod_path)

    dev_layers = _list_layers(dev_gdb)
    prod_layers = _list_layers(prod_gdb)

    mode = (
        "NULL and whitespace-only strings treated as equivalent"
        if blank_as_null
        else "STRICT: NULL and whitespace-only strings treated as distinct"
    )
    print(f"=== NULL/BLANK MODE: {mode} ===\n")

    print("=== LAYER STRUCTURE ===")
    all_layers = sorted(set(dev_layers) | set(prod_layers))
    for layer in all_layers:
        d = dev_layers.get(layer, "MISSING")
        p = prod_layers.get(layer, "MISSING")
        match = "" if d == p else "  *** MISMATCH"
        print(f"  {layer:20s}  dev={d!s:20s}  prod={p!s}{match}")

    common_layers = sorted(set(dev_layers) & set(prod_layers))
    print()

    all_col_rows: list[dict] = []
    clean_layers = 0

    for layer in common_layers:
        dev_gdf = gpd.read_file(dev_gdb, layer=layer)
        prod_gdf = gpd.read_file(prod_gdb, layer=layer)
        row_diff = len(dev_gdf) - len(prod_gdf)

        # Non-spatial layers (node_stname, altnames) read back as plain DataFrames
        # with no .crs — guard so the comparison covers them too.
        dev_crs = str(getattr(dev_gdf, "crs", None) or "None")
        prod_crs = str(getattr(prod_gdf, "crs", None) or "None")
        crs_ok = "OK" if dev_crs == prod_crs else "MISMATCH"

        dev_cols = list(dev_gdf.columns)
        prod_cols = list(prod_gdf.columns)
        missing_from_dev = sorted(set(prod_cols) - set(dev_cols))
        extra_in_dev = sorted(set(dev_cols) - set(prod_cols))
        order_ok = dev_cols == prod_cols

        attribute_cols = [
            c for c in prod_cols if c in set(dev_cols) and c != "geometry"
        ]
        if attribute_cols:
            declared = declared_keys.get(layer)
            if declared and all(c in attribute_cols for c in declared):
                key_cols = declared
            else:
                if declared:
                    print(
                        f"  WARNING: declared key {declared} for layer {layer} isn't "
                        "in this build's columns - guessing instead. Update "
                        "lion_outputs.csv's key_columns."
                    )
                else:
                    print(
                        f"  WARNING: no declared key for layer {layer} in "
                        "lion_outputs.csv - guessing one for this run only. Add a "
                        "key_columns entry once you've verified one against real data."
                    )
                key_cols = gdb_compare.guess_key_columns(
                    dev_gdf, prod_gdf, attribute_cols
                )
            compare_cols = [c for c in attribute_cols if c not in key_cols]
            row_level = gdb_compare.row_level_diff(
                dev_gdf, prod_gdf, key_cols, compare_cols, blank_as_null
            )
        else:
            # No non-geometry columns at all - fall back to a plain count.
            key_cols = []
            row_level = gdb_compare.RowLevelDiff(
                only_in_dev=max(row_diff, 0),
                only_in_prod=max(-row_diff, 0),
                modified=None,
                precise=False,
            )

        dev_area = prod_area = area_pct_diff = None
        if layer in polygon_layers:
            dev_area = dev_gdf.geometry.area.sum()
            prod_area = prod_gdf.geometry.area.sum()
            area_pct_diff = (
                (dev_area - prod_area) / prod_area * 100 if prod_area else 0.0
            )

        # Compute every column's stats first so a real attribute-level anomaly
        # (null-rate diff, ALL NULL in dev) can feed into layer_note below,
        # rather than only structure/row-count/area deciding whether a layer
        # reads as "OK".
        common_cols = [c for c in prod_cols if c in set(dev_cols)]
        col_stats = []
        for col in common_cols:
            dev_s = dev_gdf[col]
            prod_s = prod_gdf[col]

            dev_na = gdb_compare.effective_isna(dev_s, blank_as_null)
            prod_na = gdb_compare.effective_isna(prod_s, blank_as_null)
            dev_null_pct = dev_na.mean() * 100
            prod_null_pct = prod_na.mean() * 100

            dev_nunique = dev_s[~dev_na].nunique(dropna=True)
            prod_nunique = prod_s[~prod_na].nunique(dropna=True)

            note = ""
            if col == "geometry":
                note = "spatial"
            elif col in KNOWN_NULL_COLUMNS.get(layer, set()):
                if dev_null_pct == 100 and prod_null_pct < 100:
                    note = "KNOWN: unimplemented (hardcoded null)"
            elif dev_null_pct == 100 and prod_null_pct < 100:
                note = "ALL NULL in dev"
            elif abs(dev_null_pct - prod_null_pct) > 5:
                note = f"null rate diff {dev_null_pct - prod_null_pct:+.1f}pp"

            col_stats.append(
                {
                    "column": col,
                    "dev_null_pct": dev_null_pct,
                    "prod_null_pct": prod_null_pct,
                    "dev_nunique": dev_nunique,
                    "prod_nunique": prod_nunique,
                    "note": note,
                }
            )

        flagged_columns = [
            s["column"]
            for s in col_stats
            if s["note"] and s["note"] != "spatial" and "KNOWN:" not in s["note"]
        ]

        layer_note = _layer_note(
            layer=layer,
            missing_from_dev=missing_from_dev,
            extra_in_dev=extra_in_dev,
            columns_match_but_order_differs=(
                not order_ok and not missing_from_dev and not extra_in_dev
            ),
            row_level=row_level,
            area_pct_diff=area_pct_diff,
            flagged_columns=flagged_columns,
        )
        if layer_note == "OK":
            clean_layers += 1

        area_str = f"  area={area_pct_diff:+7.3f}%" if area_pct_diff is not None else ""
        key_str = f"  key=[{', '.join(key_cols)}]" if key_cols else ""
        print(
            f"=== LAYER: {layer} ===  dev={len(dev_gdf):,}  prod={len(prod_gdf):,}"
            f"  crs=[{crs_ok}]{key_str}{area_str}  {layer_note}"
        )
        print(
            f"  {'column':30s}  {'dev_nulls%':>10}  {'prod_nulls%':>11}  {'dev_nunique':>11}  {'prod_nunique':>12}  note"
        )

        for s in col_stats:
            print(
                f"  {s['column']:30s}  {s['dev_null_pct']:>9.1f}%  {s['prod_null_pct']:>10.1f}%"
                f"  {s['dev_nunique']:>11,}  {s['prod_nunique']:>12,}  {s['note']}"
            )

            all_col_rows.append(
                {
                    "layer": layer,
                    "column": s["column"],
                    "dev_row_count": len(dev_gdf),
                    "prod_row_count": len(prod_gdf),
                    "row_diff": row_diff,
                    "dev_null_pct": round(s["dev_null_pct"], 2),
                    "prod_null_pct": round(s["prod_null_pct"], 2),
                    "null_pct_diff": round(s["dev_null_pct"] - s["prod_null_pct"], 2),
                    "dev_nunique": s["dev_nunique"],
                    "prod_nunique": s["prod_nunique"],
                    "dev_area": round(dev_area) if dev_area is not None else "",
                    "prod_area": round(prod_area) if prod_area is not None else "",
                    "area_pct_diff": (
                        round(area_pct_diff, 4) if area_pct_diff is not None else ""
                    ),
                    "key_columns": ", ".join(key_cols),
                    "key_precise": row_level.precise,
                    "rows_only_in_dev": row_level.only_in_dev,
                    "rows_only_in_prod": row_level.only_in_prod,
                    "rows_modified": (
                        row_level.modified if row_level.modified is not None else ""
                    ),
                    "note": s["note"],
                    "layer_note": layer_note,
                }
            )
        print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"{report_name}_comparison.csv"
    if all_col_rows:
        with out_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_col_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_col_rows)
        print(f"{clean_layers}/{len(common_layers)} layers clean")
        print(f"Per-column CSV written to {out_csv}")


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
        print("No gdb-format exports in recipe; nothing to compare.")
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
            print(f"Fetching prod GDB s3://{PROD_BUCKET}/{key}")
            s3.download_file(PROD_BUCKET, key, prod_path)

        print(f"\ndev:  {dev_path.resolve()}")
        print(f"prod: {prod_path.resolve()}\n")
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
