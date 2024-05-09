from pathlib import Path
import typer

import dcpy.models.product.dataset.metadata as m
from dcpy.lifecycle.package import validate as v
from dcpy.utils import s3
from dcpy.utils.logging import logger
import dcpy.connectors.edm.packaging as packaging
import dcpy.connectors.socrata.publish as soc_pub

# Adding two apps here to achieve nested commands
# ie. dcpy.cli lifecycle distribute socrata from_s3
socrata_app = typer.Typer()

distribute_app = typer.Typer()
distribute_app.add_typer(socrata_app, name="socrata")


@socrata_app.command("from_local")
def _dist_from_local(
    package_path: Path = typer.Option(
        None,
        "-m",
        "--metadata-path",
        help="(Optional) Metadata Path",
    ),
    dataset_destination_id: str = typer.Option(
        None,
        "-d",
        "--dest",
        help="Dataset Destination Id",
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
):
    md = m.Metadata.from_yaml(metadata_path or package_path / "metadata.yml")
    dest = md.get_destination(dataset_destination_id)
    assert dest.type == "socrata"

    if not skip_validation:
        logger.info("Validating package")
        validation = v.validate_package(package_path, md)
        errors = validation.get_dataset_errors()

        if len(errors) > 0:
            if ignore_validation_errors:
                logger.warn("Errors Found! But continuing to distribute")
                validation.pretty_print_errors()
            else:
                error_msg = "Errors Found! Aborting distribute"
                logger.error(error_msg)
                validation.pretty_print_errors()
                raise Exception(error_msg)

    ds_name_to_push = dest.datasets[0]  # socrata will only have one dataset

    match md.package.get_dataset(ds_name_to_push).type:
        case "shapefile":
            soc_pub.push_shp(md, dataset_destination_id, package_path, publish=publish)
        case _:
            # TODO
            raise Exception("Only shapefiles have been implemented so far")


@socrata_app.command("from_s3")
def _dist_from_s3(
    product_name: str,
    version: str,
    dataset_destination_id: str,
    dataset: str = typer.Option(
        None,
        "-d",  # d is already taken, unfortunately
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
):
    logger.info(
        f"Distributing {product_name}-{version} to {dataset_destination_id}. Publishing: {publish}. Ignoring Validation Errors: {ignore_validation_errors}"
    )
    logger.info(f"Downloading dataset package for {product_name}-{version}")

    package_path = packaging.download_packaged_version(
        packaging.PackageKey(product_name, version, dataset or product_name)
    )

    _dist_from_local(
        package_path=package_path,
        dataset_destination_id=dataset_destination_id,
        metadata_path=metadata_path,
        publish=publish,
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
    )
