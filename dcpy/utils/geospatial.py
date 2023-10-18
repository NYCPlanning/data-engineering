# ? move this file to dcpy.builds?
from numpy import floor
from osgeo import gdal
import typer
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from dcpy.utils.logging import logger

PRODUCT_MVT_FIELDS = {
    "pluto": [
        "BBL",
        "LandUse",
        "LotType",
        "NumBldgs",
    ]
}


def translate_shp_to_mvt(
    product: str, input_path: str, min_zoom: int = 0, max_zoom: int = 5
) -> None:
    """Keeping scope of this very limited - should be refactored once data library is fully brought in"""
    output_path = f"{product}_mvt"
    select_fields = (
        PRODUCT_MVT_FIELDS[product] if product in PRODUCT_MVT_FIELDS.keys() else None
    )
    logger.info(f"Generating MVTs for {product} using {input_path}")
    logger.info(f"Based on product name, limited to fields {select_fields}")

    with Progress(
        SpinnerColumn(spinner_name="earth"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        transient=False,
    ) as progress:
        task = progress.add_task(
            f"[green]Translating [bold]{input_path}[/bold]", total=1000
        )

        def update_progress(complete, message, unknown):
            progress.update(task, completed=floor(complete * 1000))

        gdal.UseExceptions()
        gdal.VectorTranslate(
            output_path,
            f"/vsizip/{input_path}",
            format="MVT",
            accessMode="overwrite",
            makeValid=True,
            selectFields=select_fields,
            # optional settings
            callback=update_progress,
            datasetCreationOptions=[f"MINZOOM={min_zoom}", f"MAXZOOM={max_zoom}"],
        )


app = typer.Typer(add_completion=False)


@app.command()
def _cli_wrapper_translate(
    product: str = typer.Option(
        None,
        "-p",
        "--product",
    ),
    input_path: str = typer.Option(
        None,
        "-i",
        "--input-path",
    ),
    min_zoom: int = typer.Option(
        None,
        "--min-zoom",
    ),
    max_zoom: int = typer.Option(
        None,
        "--max-zoom",
    ),
):
    translate_shp_to_mvt(product, input_path, min_zoom, max_zoom)


if __name__ == "__main__":
    app()
