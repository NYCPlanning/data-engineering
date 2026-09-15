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

Report-only: writes a per-column CSV per gdb to output/validation_output/<name>_comparison.csv
and prints a report to stdout. It never fails the build on a data mismatch.
"""

import csv
import zipfile
from collections import Counter
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyogrio
import typer

from dcpy.lifecycle.builds import plan
from dcpy.utils import s3

OUTPUT_DIR = Path("output/validation_output")
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


def _find_key_columns(
    dev_df: pd.DataFrame, prod_df: pd.DataFrame, candidate_cols: list[str]
) -> list[str]:
    """A single column unique in both dev and prod, if one exists (e.g. SegmentID
    on the lion layer, NODEID on node); otherwise every candidate column together,
    treating the full attribute tuple as the row's identity (e.g. node_stname,
    keyed on (NODEID, STNAME) since neither alone is unique)."""
    for col in candidate_cols:
        if dev_df[col].nunique(dropna=False) == len(dev_df) and prod_df[col].nunique(
            dropna=False
        ) == len(prod_df):
            return [col]
    return candidate_cols


_NA_SENTINEL = "\x00NA\x00"


def _composite_key(df: pd.DataFrame, key_cols: list[str]) -> pd.Series:
    if df.empty:
        # .agg(..., axis=1) on a 0-row frame returns the frame unchanged rather
        # than an empty Series - nyura (both sides empty) hits this for real.
        return pd.Series([], dtype=str)
    # fillna before astype(str), not after: pandas' string dtype (default since
    # pandas 3.0) makes astype(str) preserve NaN as a true null rather than the
    # old behavior of stringifying it to "nan" - "|".join then chokes on the
    # leftover float. Filling first guarantees every cell is a real string by
    # the time join sees it, for columns with real nulls (most gdb attributes).
    return df[key_cols].fillna(_NA_SENTINEL).astype(str).agg("|".join, axis=1)


def _row_level_diff(
    dev_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    key_cols: list[str],
    compare_cols: list[str],
) -> dict:
    """Keyed row-level diff: how many rows exist only in dev, only in prod, or on
    both sides but with a differing attribute value.

    "modified" is only meaningful when key_cols uniquely identifies a row on both
    sides (precise=True) - if _find_key_columns fell back to the full attribute
    tuple, two rows can never differ while sharing a key by construction, so
    every real disagreement shows up as an add+remove pair instead, and this
    falls back to a duplicate-tolerant multiset comparison (same approach as
    qa__ldf_summary - counts, not row-for-row pairing).
    """
    dev_keys = _composite_key(dev_df, key_cols)
    prod_keys = _composite_key(prod_df, key_cols)
    precise = dev_keys.is_unique and prod_keys.is_unique

    if not precise:
        dev_counts = Counter(dev_keys)
        prod_counts = Counter(prod_keys)
        only_in_dev = sum(
            max(c - prod_counts.get(k, 0), 0) for k, c in dev_counts.items()
        )
        only_in_prod = sum(
            max(c - dev_counts.get(k, 0), 0) for k, c in prod_counts.items()
        )
        return {
            "only_in_dev": only_in_dev,
            "only_in_prod": only_in_prod,
            "modified": None,
            "precise": False,
        }

    dev_indexed = dev_df.set_index(dev_keys)
    prod_indexed = prod_df.set_index(prod_keys)
    only_in_dev = dev_indexed.index.difference(prod_indexed.index)
    only_in_prod = prod_indexed.index.difference(dev_indexed.index)
    common = dev_indexed.index.intersection(prod_indexed.index)

    modified = 0
    if len(common) > 0 and compare_cols:
        # fillna before astype(str) - see _composite_key. Without it, two cells
        # that are both genuinely null compare as "different" (float NaN is
        # never equal to itself), which would inflate modified on any layer
        # with shared-null attributes - not just a crash risk like in the key.
        dev_common = (
            dev_indexed.loc[common, compare_cols].fillna(_NA_SENTINEL).astype(str)
        )
        prod_common = (
            prod_indexed.loc[common, compare_cols].fillna(_NA_SENTINEL).astype(str)
        )
        modified = int((dev_common.values != prod_common.values).any(axis=1).sum())

    return {
        "only_in_dev": len(only_in_dev),
        "only_in_prod": len(only_in_prod),
        "modified": modified,
        "precise": True,
    }


def _layer_note(
    *,
    layer: str,
    missing_from_dev: list[str],
    extra_in_dev: list[str],
    columns_match_but_order_differs: bool,
    row_level: dict,
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
        row_level["only_in_dev"]
        + row_level["only_in_prod"]
        + (row_level["modified"] or 0)
    )
    if total_diff:
        if row_level["precise"]:
            flags.append(
                f"{total_diff:,} rows differ ({row_level['modified']:,} modified, "
                f"{row_level['only_in_dev']:,} dev-only, "
                f"{row_level['only_in_prod']:,} prod-only)"
            )
        else:
            flags.append(
                f"{total_diff:,} rows differ ({row_level['only_in_dev']:,} dev-only, "
                f"{row_level['only_in_prod']:,} prod-only)"
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
    dev_path: Path, prod_path: Path, report_name: str, polygon_layers: set[str]
) -> None:
    dev_gdb = _inner_gdb(dev_path)
    prod_gdb = _inner_gdb(prod_path)

    dev_layers = _list_layers(dev_gdb)
    prod_layers = _list_layers(prod_gdb)

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
            key_cols = _find_key_columns(dev_gdf, prod_gdf, attribute_cols)
            compare_cols = [c for c in attribute_cols if c not in key_cols]
            row_level = _row_level_diff(dev_gdf, prod_gdf, key_cols, compare_cols)
        else:
            # No non-geometry columns at all - fall back to a plain count.
            key_cols = []
            row_level = {
                "only_in_dev": max(row_diff, 0),
                "only_in_prod": max(-row_diff, 0),
                "modified": None,
                "precise": False,
            }

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

            dev_null_pct = dev_s.isna().mean() * 100
            prod_null_pct = prod_s.isna().mean() * 100

            dev_nunique = dev_s.nunique(dropna=True)
            prod_nunique = prod_s.nunique(dropna=True)

            note = ""
            if col == "geometry":
                note = "spatial"
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
            s["column"] for s in col_stats if s["note"] and s["note"] != "spatial"
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
                    "key_precise": row_level["precise"],
                    "rows_only_in_dev": row_level["only_in_dev"],
                    "rows_only_in_prod": row_level["only_in_prod"],
                    "rows_modified": (
                        row_level["modified"]
                        if row_level["modified"] is not None
                        else ""
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
            dev_path, prod_path, Path(filename).name.split(".")[0], polygon_layers
        )


if __name__ == "__main__":
    app()
