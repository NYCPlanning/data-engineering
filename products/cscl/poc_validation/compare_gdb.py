"""
Compare dev and prod LION GDB layers for structure and data parity.

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
from pathlib import Path

import geopandas as gpd
import pyogrio
import typer

from dcpy.lifecycle.builds import plan
from dcpy.utils import s3

OUTPUT_DIR = Path("output/validation_output")
PROD_BUCKET = "edm-private"

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


def _compare_layers(dev_path: Path, prod_path: Path, report_name: str) -> None:
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

    for layer in common_layers:
        print(f"=== LAYER: {layer} ===")
        dev_gdf = gpd.read_file(dev_gdb, layer=layer)
        prod_gdf = gpd.read_file(prod_gdb, layer=layer)

        print(f"  rows     dev={len(dev_gdf):,}  prod={len(prod_gdf):,}")
        # Non-spatial layers (node_stname, altnames) read back as plain DataFrames
        # with no .crs — guard so the comparison covers them too.
        dev_crs = str(getattr(dev_gdf, "crs", None) or "None")
        prod_crs = str(getattr(prod_gdf, "crs", None) or "None")
        crs_ok = "OK" if dev_crs == prod_crs else "MISMATCH"
        print(f"  crs      dev={dev_crs}  prod={prod_crs}  [{crs_ok}]")

        dev_cols = list(dev_gdf.columns)
        prod_cols = list(prod_gdf.columns)
        missing_from_dev = sorted(set(prod_cols) - set(dev_cols))
        extra_in_dev = sorted(set(dev_cols) - set(prod_cols))
        order_ok = dev_cols == prod_cols

        print(f"  columns  dev={len(dev_cols)}  prod={len(prod_cols)}")
        if missing_from_dev:
            print(f"  missing from dev: {missing_from_dev}")
        if extra_in_dev:
            print(f"  extra in dev:     {extra_in_dev}")
        if not order_ok and not missing_from_dev and not extra_in_dev:
            print("  column ORDER differs")

        common_cols = [c for c in prod_cols if c in set(dev_cols)]
        print()
        print(
            f"  {'column':30s}  {'dev_nulls%':>10}  {'prod_nulls%':>11}  {'dev_nunique':>11}  {'prod_nunique':>12}  note"
        )

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

            print(
                f"  {col:30s}  {dev_null_pct:>9.1f}%  {prod_null_pct:>10.1f}%"
                f"  {dev_nunique:>11,}  {prod_nunique:>12,}  {note}"
            )

            all_col_rows.append(
                {
                    "layer": layer,
                    "column": col,
                    "dev_row_count": len(dev_gdf),
                    "prod_row_count": len(prod_gdf),
                    "dev_null_pct": round(dev_null_pct, 2),
                    "prod_null_pct": round(prod_null_pct, 2),
                    "null_pct_diff": round(dev_null_pct - prod_null_pct, 2),
                    "dev_nunique": dev_nunique,
                    "prod_nunique": prod_nunique,
                    "note": note,
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
    """Compare dev vs prod LION GDB layers for each gdb export in the recipe."""
    recipe = plan.recipe_from_yaml(recipe_path)
    assert recipe.exports, "recipe has no exports"
    gdb_filenames = sorted(
        {
            e.filename
            for e in recipe.exports.datasets
            if e.format.value == "gdb" and e.filename
        }
    )
    if not gdb_filenames:
        print("No gdb-format exports in recipe; nothing to compare.")
        return

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
        _compare_layers(dev_path, prod_path, Path(filename).name.split(".")[0])


if __name__ == "__main__":
    app()
