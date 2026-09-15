import typer

from dcpy.data import compare
from dcpy.lifecycle.builds.artifacts import builds
from dcpy.utils.collections import indented_report

app = typer.Typer()

DEFAULT_PROD_BUILD_NAME = "nightly_qa"


def _load_export(product: str, build: str, filename: str):
    """Loads a published build export, dispatching on file type.

    CSVs load as a plain DataFrame. Shapefiles load as a GeoDataFrame with the
    geometry column converted to WKB hex first, so the same key/column diffing
    logic in dcpy.data.compare works unmodified for both -- geometry becomes
    just another comparable string column, no separate spatial-equality path
    needed here.
    """
    if filename.endswith(".shp.zip"):
        gdf = builds.read_shapefile(product, build, filename)
        geom_col = gdf.geometry.name
        gdf[geom_col] = gdf[geom_col].apply(
            lambda geom: geom.wkb_hex if geom is not None else None
        )
        return gdf
    elif filename.endswith(".csv"):
        return builds.read_csv(product, build, filename)
    else:
        raise ValueError(
            f"Don't know how to compare file type for '{filename}' "
            "(expected .csv or .shp.zip)"
        )


def comparison_report(
    product: str,
    build_name_dev: str,
    file_name_dev: str,
    build_name_prod: str,
    file_name_prod: str,
    key_columns: list[str],
) -> None:
    dev = _load_export(product, build_name_dev, file_name_dev)
    prod = _load_export(product, build_name_prod, file_name_prod)
    print(
        f"Comparing {product}/build/{build_name_dev}/{file_name_dev} (dev) "
        f"to {product}/build/{build_name_prod}/{file_name_prod} (prod)"
    )
    report = compare.get_df_keyed_report(dev, prod, key_columns)
    print(indented_report(report.model_dump(), include_line_breaks=True))


@app.command("compare")
def _compare_cli(
    product: str = typer.Argument(
        help="The product name in edm-publishing, e.g. db-cpdb"
    ),
    build_name_dev: str = typer.Argument(help="The dev build name"),
    file_name: str = typer.Argument(
        help="The prod and (if identical) dev exported file name, e.g. "
        "cpdb_projects.csv or cpdb_dcpattributes_pts.shp.zip"
    ),
    key_columns: list[str] = typer.Option(
        ...,
        "--key-column",
        help="Column(s) to join dev/prod rows on, e.g. maprojid. Repeatable.",
    ),
    build_name_prod: str = typer.Option(
        DEFAULT_PROD_BUILD_NAME, help="The prod build name"
    ),
    file_name_dev: str | None = typer.Option(
        None, help="The dev file name (default: the prod file name)"
    ),
) -> None:
    if not file_name_dev:
        file_name_dev = file_name
    comparison_report(
        product, build_name_dev, file_name_dev, build_name_prod, file_name, key_columns
    )
