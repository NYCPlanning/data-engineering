from pathlib import Path

import typer

from dcpy.lifecycle import distribute

app = typer.Typer()


@app.command("from_local")
def _dist_from_local(
    product: str,
    dataset: str,
    version: str,
    package_path: Path = typer.Argument(),
    dataset_destination_id: str = typer.Argument(),
    publish: bool = typer.Option(
        False,
        "-p",
        "--publish",
        help="Publish the Revision? Or leave it open.",
    ),
    metadata_only: bool = typer.Option(
        False,
        "-z",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    distribute.to_dataset_destination(
        product=product,
        dataset=dataset,
        version=version,
        destination_id=dataset_destination_id,
        publish=publish,
        package_path=package_path,
        metadata_only=metadata_only,
    )
