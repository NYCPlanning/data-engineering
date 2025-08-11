from pathlib import Path
import typer

from .validate import _validate
from .esri import app as esri_app

from .assemble import assemble_dataset_package
from .xlsx_writer import app as xlsx_writer_app
from .shapefiles import app as shapefile_app

app = typer.Typer()
app.command(name="validate")(_validate)
app.add_typer(esri_app, name="esri")
app.add_typer(xlsx_writer_app, name="oti")
app.add_typer(shapefile_app, name="shapefile")


@app.command()
def assemble(
    dataset_key: str,
    version: str,
    source: str,
    path_override: Path = typer.Option(
        None,
        "--output-path",
        "-o",
        help="Output Path. Defaults to path configured in lifecycle.config",
    ),
    metadata_only: bool = typer.Option(
        False,
        "--metadata-only",
        "-m",
        help="Only Assemble Metadata. E.g. PDFs, Excel files, etc",
    ),
):
    product, *rest = dataset_key.split(".")
    dataset = rest[0] if rest else product  # e.g. lion.lion
    result = assemble_dataset_package(
        product=product,
        dataset=dataset,
        source_id=source,
        version=version,
        path_override=path_override,
        metadata_only=metadata_only,
    )

    if not result.success:
        error_msg = f"Encountered Errors assembling {dataset_key} from {source}: {result.result_summary} - {result.result_details}"
        typer.echo(error_msg)
        raise Exception(error_msg)
    else:
        typer.echo(result.package_path)
