from pathlib import Path
import typer
from typing import TypedDict, NotRequired, Required

import dcpy.models.product.dataset.metadata as m
import dcpy.connectors.socrata.publish as soc_pub


class PublishKwargs(TypedDict):
    metadata_path: NotRequired[Path]
    publish: Required[bool]
    ignore_validation_errors: Required[bool]
    skip_validation: Required[bool]
    metadata_only: Required[bool]


def dist_from_local(
    package_path: Path,
    dataset_destination_id: str,
    *,
    metadata_path: Path | None = None,
    publish: bool = False,
    metadata_only: bool = False,
) -> str:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    md = m.Metadata.from_path(metadata_path or (package_path / "metadata.yml"))
    dest = md.get_destination(dataset_destination_id)
    # TODO generalize to not be socrata only

    try:
        return soc_pub.push_dataset(
            metadata=md,
            dataset_destination_id=dataset_destination_id,
            dataset_package_path=package_path,
            publish=bool(publish),
            metadata_only=bool(metadata_only),
        )
    except Exception as e:
        return f"Error pushing {md.attributes.display_name}, destination: {dest.id}: {str(e)}"


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
    result = dist_from_local(
        package_path=package_path,
        dataset_destination_id=dataset_destination_id,
        metadata_path=metadata_path,
        publish=publish,
        metadata_only=metadata_only,
    )
    print(result)
