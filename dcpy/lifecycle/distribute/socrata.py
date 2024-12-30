from pathlib import Path
import typer
from typing import TypedDict, Unpack, NotRequired, Required

import dcpy.models.product.dataset.metadata as m
from dcpy.utils.logging import logger
import dcpy.connectors.edm.packaging as packaging
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
    ignore_validation_errors: bool = False,
    skip_validation: bool = False,
    metadata_only: bool = False,
) -> str:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    md = m.Metadata.from_path(metadata_path or (package_path / "metadata.yml"))
    validation_errors = md.validate_consistency()
    if validation_errors:
        logger.error(
            f"The metadata file contains inconsistencies that must be fixed before pushing: {str(validation_errors)}"
        )
        return str(validation_errors)

    dest = md.get_destination(dataset_destination_id)
    assert dest.type == "socrata"

    if not (skip_validation or metadata_only):
        logger.info("Validating package")
        # validation = v.validate_package(package_path, md)
        # errors = validation.get_dataset_errors()
        errors: list[str] = []  # TODO. Not really used, so not urgent

        if len(errors) > 0:
            if ignore_validation_errors:
                logger.warn("Errors Found! But continuing to distribute")
                # validation.pretty_print_errors()
            else:
                error_msg = "Errors Found! Aborting distribute"
                logger.error(error_msg)
                # validation.pretty_print_errors()
                raise Exception(error_msg)

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


def dist_from_local_all_socrata(
    package_path: Path,
    **pub_kwargs: Unpack[PublishKwargs],
):
    """Distributes all Socrata destinations within a given metadata"""
    md = m.Metadata.from_path(package_path / "metadata.yml")
    local_pub_kwargs = pub_kwargs.copy()
    local_pub_kwargs.pop(
        "metadata_path"
    ) if "metadata_path" in local_pub_kwargs else None

    socrata_dests = [d.id for d in md.destinations if d.type == "socrata"]
    logger.info(f"Distributing {md.attributes.display_name}: {socrata_dests}")
    results = [
        dist_from_local(
            package_path=package_path,
            dataset_destination_id=dataset_destination_id,
            **local_pub_kwargs,
        )
        for dataset_destination_id in socrata_dests
    ]
    return results


def dist_from_local_product_all_socrata(
    product_path: Path,
    **pub_kwargs: Unpack[PublishKwargs],
):
    """Distribute datasets for an entire product."""
    results = []
    for p in product_path.iterdir():
        if p.is_dir():
            md_path = p / "metadata.yml"
            if md_path.exists():
                results.append(
                    dist_from_local_all_socrata(package_path=Path(p), **pub_kwargs)
                )
    return results


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
    ignore_validation_errors: bool = typer.Option(
        False,
        "-i",
        "--ignore-validation-errors",
        help="Ignore Validation Errors? Will still perform validation, but ignore errors, allowing a push",
    ),
    skip_validation: bool = typer.Option(
        False,
        "-y",  # -y(olo)
        "--skip-validation",
        help="Skip Validation Altogether",
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
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
        metadata_only=metadata_only,
    )
    print(result)


@socrata_app.command("local_all_datasets")
def _dist_from_local_all_socrata(
    package_path: Path = typer.Argument(),
    publish: bool = typer.Option(
        False,
        "-p",
        "--publish",
        help="Publish the Socrata Revision? Or leave it open.",
    ),
    ignore_validation_errors: bool = typer.Option(
        False,
        "-i",
        "--ignore-validation-errors",
        help="Ignore Validation Errors? Will still perform validation, but ignore errors, allowing a push",
    ),
    skip_validation: bool = typer.Option(
        False,
        "-y",  # -y(olo)
        "--skip-validation",
        help="Skip Validation Altogether",
    ),
    metadata_only: bool = typer.Option(
        False,
        "-z",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    results = dist_from_local_all_socrata(
        package_path=package_path,
        publish=publish,
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
        metadata_only=metadata_only,
    )
    print(results)


@socrata_app.command("local_all_product")
def _cli_dist_product_from_local_all_socrata(
    product_path: Path = typer.Argument(),
    publish: bool = typer.Option(
        False,
        "-p",
        "--publish",
        help="Publish the Socrata Revision? Or leave it open.",
    ),
    ignore_validation_errors: bool = typer.Option(
        False,
        "-i",
        "--ignore-validation-errors",
        help="Ignore Validation Errors? Will still perform validation, but ignore errors, allowing a push",
    ),
    skip_validation: bool = typer.Option(
        False,
        "-y",  # -y(olo)
        "--skip-validation",
        help="Skip Validation Altogether",
    ),
    metadata_only: bool = typer.Option(
        False,
        "-z",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    results = dist_from_local_product_all_socrata(
        product_path=product_path,
        publish=publish,
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
        metadata_only=metadata_only,
    )
    print(results)


@socrata_app.command("from_s3")
def _dist_from_s3(
    product_name: str,
    version: str,
    dataset_destination_id: str,
    dataset: str = typer.Option(
        None,
        "-d",
        "--dataset",
        help="(optional) dataset. Defaults to product name",
    ),
    metadata_path: Path = typer.Option(
        None,
        "-m",
        "--metadata-path",
        help="(Optional) Metadata Path",
    ),
    publish: bool = typer.Option(
        False,
        "-p",
        "--publish",
        help="Publish the Socrata Revision? Or leave it open.",
    ),
    ignore_validation_errors: bool = typer.Option(
        False,
        "-i",
        "--ignore-validation-errors",
        help="Ignore Validation Errors? Will still perform validation, but ignore errors, allowing a push",
    ),
    skip_validation: bool = typer.Option(
        False,
        "-y",  # -y(olo)
        "--skip-validation",
        help="Skip Validation Altogether",
    ),
    metadata_only: bool = typer.Option(
        False,
        "-z",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    logger.info(
        f"Distributing {product_name}-{version} to {dataset_destination_id}. Publishing: {publish}. Ignoring Validation Errors: {ignore_validation_errors}"
    )
    logger.info(f"Downloading dataset package for {product_name}-{version}")

    package_path = packaging.pull(
        packaging.DatasetPackageKey(product_name, version, dataset or product_name)
    )

    result = dist_from_local(
        package_path=package_path,
        dataset_destination_id=dataset_destination_id,
        metadata_path=metadata_path,
        publish=publish,
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
        metadata_only=metadata_only,
    )
    print(result)
