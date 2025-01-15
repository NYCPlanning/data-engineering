from pathlib import Path
import typer
from typing import Unpack

from dcpy.models.lifecycle.distribution import PublisherPushKwargs
import dcpy.models.product.dataset.metadata as m
from dcpy.lifecycle.distribute import dispatcher


def dist_from_local(**pub_kwargs: Unpack[PublisherPushKwargs]) -> str:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    # TODO generalize to not be socrata only
    dest = pub_kwargs["metadata"].get_destination(pub_kwargs["dataset_destination_id"])
    dest_type = dest.type
    try:
        return dispatcher.push(dest_type, pub_kwargs)
    except Exception as e:
        return f"Error pushing {pub_kwargs["metadata"].attributes.display_name}, destination: {dest.id}: {str(e)}"


socrata_app = typer.Typer()


@socrata_app.command("from_local")
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
        help="Publish the Socrata Revision? Or leave it open.",
    ),
    metadata_only: bool = typer.Option(
        False,
        "-z",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    md = m.Metadata.from_path(metadata_path or (package_path / "metadata.yml"))
    result = dist_from_local(
        metadata=md,
        dataset_destination_id=dataset_destination_id,
        publish=publish,
        dataset_package_path=package_path,
        metadata_only=metadata_only,
    )
    print(result)
