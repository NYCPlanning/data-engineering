"""
Compare dev and prod LION GDB layers for structure and data parity.

Run from the products/cscl directory:
    python poc_validation/compare_gdb.py

By default compares:
    dev:  output/nyclion_26a.zip      (from the local build)
    prod: ../../.task-pipeline/nyclion_26a.zip  (or pass --prod)

Outputs a text report to stdout and a per-column CSV to
output/validation_output/gdb_comparison.csv.
"""

import csv
import zipfile
from pathlib import Path

import geopandas as gpd
import pyogrio
import typer

OUTPUT_DIR = Path("output/validation_output")

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


def _compare_layers(dev_path: Path, prod_path: Path) -> None:
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
        dev_crs = str(dev_gdf.crs) if dev_gdf.crs else "None"
        prod_crs = str(prod_gdf.crs) if prod_gdf.crs else "None"
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
    out_csv = OUTPUT_DIR / "gdb_comparison.csv"
    if all_col_rows:
        with out_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(all_col_rows[0].keys()))
            writer.writeheader()
            writer.writerows(all_col_rows)
        print(f"Per-column CSV written to {out_csv}")


@app.command()
def run(
    dev: Path = typer.Option(
        Path("output/nyclion_26a.zip"),
        "--dev",
        "-d",
        help="Path to dev GDB zip",
    ),
    prod: Path = typer.Option(
        Path("../../.task-pipeline/nyclion_26a.zip"),
        "--prod",
        "-p",
        help="Path to prod GDB zip",
    ),
) -> None:
    """Compare dev and prod LION GDB layers."""
    print(f"dev:  {dev.resolve()}")
    print(f"prod: {prod.resolve()}")
    print()
    _compare_layers(dev, prod)


if __name__ == "__main__":
    app()
