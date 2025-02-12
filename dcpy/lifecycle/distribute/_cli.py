import typer
from pathlib import Path

from dcpy.lifecycle import distribute
import dcpy.models.product.dataset.metadata as m

app = typer.Typer()


@app.command("from_local")
def _dist_from_local(
    package_path: Path = typer.Argument(),
    dataset_destination_id: str = typer.Argument(),
    metadata_path: Path = typer.Option(
        None,
        "-m",
        "--metadata-path",
        help="(Optional) Metadata Path Override",
    ),
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
    md = m.Metadata.from_path(metadata_path or (package_path / "metadata.yml"))
    result = distribute.to_dataset_destination(
        metadata=md,
        dataset_destination_id=dataset_destination_id,
        publish=publish,
        dataset_package_path=package_path,
        metadata_only=metadata_only,
    )
    print(result)
