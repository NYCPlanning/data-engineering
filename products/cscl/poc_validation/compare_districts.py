"""Compare the dev district boundary gdb against the archived prod one.

Run from the products/cscl directory:
    python poc_validation/compare_districts.py

    dev:  output/dataset_files/v26B_Districts.gdb.zip   (from the local/CI build)
    prod: edm-private/cscl_etl/<version>/<filename>      (fetched to .data/prod/)

For local iteration without S3 access, pass a local prod copy with --prod.

Report-only: writes output/validation_output/district_comparison.csv and prints a
report. It never fails the build on a mismatch.
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

# Prod's nyura carries a stale copy of nybid's schema; both are empty, so the
# structural diff on this layer is expected. See models/product/districts/gdb_nyura.sql.
KNOWN_STRUCTURAL_DIFFS = {"nyura"}

app = typer.Typer(add_completion=False)


def _inner_gdb(zip_path: Path) -> str:
    """Return the /vsizip/... path to the .gdb inside a zip."""
    abs_path = str(zip_path.resolve())
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
    for n in names:
        parts = n.split("/")
        for i, p in enumerate(parts):
            if p.endswith(".gdb"):
                return f"/vsizip/{abs_path}/{'/'.join(parts[: i + 1])}"
    # Some gdb zips carry the table files at the archive root.
    return f"/vsizip/{abs_path}"


def _layers(gdb: str) -> set[str]:
    return {str(row[0]) for row in pyogrio.list_layers(gdb)}


def _compare(dev_path: Path, prod_path: Path) -> None:
    dev_gdb, prod_gdb = _inner_gdb(dev_path), _inner_gdb(prod_path)
    dev_layers, prod_layers = _layers(dev_gdb), _layers(prod_gdb)

    print("=== LAYERS ===")
    only_prod = sorted(prod_layers - dev_layers)
    only_dev = sorted(dev_layers - prod_layers)
    if only_prod:
        print(f"  missing from dev: {only_prod}")
    if only_dev:
        print(f"  extra in dev:     {only_dev}")
    if not only_prod and not only_dev:
        print(f"  {len(dev_layers)} layers, identical set")
    print()

    rows: list[dict] = []
    for layer in sorted(dev_layers & prod_layers):
        dev = gpd.read_file(dev_gdb, layer=layer)
        prod = gpd.read_file(prod_gdb, layer=layer)

        # SHAPE_Length/SHAPE_Area are real fields here: ESRI maintains them in prod,
        # and our models compute them, so they are compared like any other column.
        dev_cols = [c for c in dev.columns if c != "geometry"]
        prod_cols = [c for c in prod.columns if c != "geometry"]
        missing = sorted(set(prod_cols) - set(dev_cols))
        extra = sorted(set(dev_cols) - set(prod_cols))

        row_diff = len(dev) - len(prod)
        dev_area, prod_area = dev.geometry.area.sum(), prod.geometry.area.sum()
        area_pct = (dev_area - prod_area) / prod_area * 100 if prod_area else 0.0

        flags = []
        if missing:
            flags.append(f"missing {missing}")
        if extra:
            flags.append(f"extra {extra}")
        if not missing and not extra and dev_cols != prod_cols:
            flags.append("column ORDER differs")
        if row_diff:
            flags.append(f"rows {row_diff:+,}")
        if abs(area_pct) > 0.5:
            flags.append(f"area {area_pct:+.2f}%")
        note = "; ".join(flags) or "OK"
        if flags and layer in KNOWN_STRUCTURAL_DIFFS:
            note = f"KNOWN: {note}"

        print(
            f"  {layer:12s} dev={len(dev):>7,}  prod={len(prod):>7,}"
            f"  area={area_pct:+7.3f}%  {note}"
        )
        rows.append({
            "layer": layer, "dev_rows": len(dev), "prod_rows": len(prod),
            "row_diff": row_diff, "dev_area": round(dev_area), "prod_area": round(prod_area),
            "area_pct_diff": round(area_pct, 4), "missing_columns": ", ".join(missing),
            "extra_columns": ", ".join(extra), "note": note,
        })

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / "district_comparison.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    clean = sum(1 for r in rows if r["note"] == "OK")
    print(f"\n{clean}/{len(rows)} layers match on structure, row count and area")
    print(f"Per-layer CSV written to {out_csv}")


@app.command()
def run(
    recipe_path: Path = typer.Option(Path("recipe.yml"), "--recipe", "-r"),
    prod_version: str | None = typer.Option(None, "--prod-version", "-v"),
    dev: Path | None = typer.Option(None, "--dev", "-d"),
    prod: Path | None = typer.Option(None, "--prod", "-p"),
) -> None:
    """Compare the dev district gdb against prod, layer by layer."""
    recipe = plan.recipe_from_yaml(recipe_path)
    assert recipe.exports, "recipe has no exports"

    filenames = sorted({
        e.filename
        for e in recipe.exports.datasets
        if e.format.value == "gdb" and e.filename and "Districts" in e.filename
    })
    if not filenames:
        print("No district gdb exports in recipe; nothing to compare.")
        return
    filename = filenames[0]

    dev_path = dev if dev is not None else Path("output") / "dataset_files" / filename
    if prod is not None:
        prod_path = prod
    else:
        prod_path = Path(".data/prod") / filename
        key = f"cscl_etl/{prod_version or recipe.version}/{filename}"
        if not prod_path.exists():
            print(f"Fetching prod gdb s3://{PROD_BUCKET}/{key}")
            s3.download_file(PROD_BUCKET, key, prod_path)

    print(f"dev:  {dev_path.resolve()}")
    print(f"prod: {prod_path.resolve()}\n")
    _compare(dev_path, prod_path)


if __name__ == "__main__":
    app()
