from pathlib import Path
import typer

import dcpy.models.product_metadata as m
from dcpy.lifecycle.package import validate as v
from dcpy.utils import s3
from dcpy.utils.logging import logger
import dcpy.connectors.edm.packaging as packaging
import dcpy.connectors.socrata.publish as soc_pub

app = typer.Typer(add_completion=False)


@app.command("placeholder")
def _placeholder():
    assert False, "Come on, now..."


@app.command("from_local")
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
):
    md = m.Metadata.from_yaml(metadata_path or package_path / "metadata.yml")
    dest = md.get_destination(dataset_destination_id)
    assert dest.type == "socrata"

    logger.info("Validating package")
    try:
        errors = v.validate_package(package_path, md)
    except Exception as e:
        errors = [str(e)]

    if len(errors) > 0:
        error_msg = f"Errors Found! {errors}"
        if ignore_validation_errors:
            logger.warn(error_msg)
        else:
            logger.error(error_msg)
            raise Exception(error_msg)

    ds_name_to_push = dest.datasets[0]  # socrata will only have one dataset

    match md.dataset_package.get_dataset(ds_name_to_push).type:
        case "shapefile":
            soc_pub.push_shp(md, dataset_destination_id, package_path, publish=publish)

        case _:
            # TODO
            raise Exception("Only shapefiles have been implemented so far")


@app.command("from_s3")
def _dist_from_s3(
    product_name: str = typer.Option(
        None,
        "-n",
        "--name",
        help="product name",
    ),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="package version",
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
):
    logger.info(
        f"Distributing {product_name}-{version} to {dataset_destination_id}. Publishing: {publish}. Ignoring Validation Errors: {ignore_validation_errors}"
    )
    download_root_path = Path(".packaged/")
    package_path = download_root_path / product_name / version
    logger.info(f"Downloading dataset package for {product_name}-{version}")
    s3.download_folder(
        packaging.BUCKET,
        f"{product_name}/{version}/",
        package_path,
        include_prefix_in_export=False,
    )
    _dist_from_local(
        package_path=package_path,
        dataset_destination_id=dataset_destination_id,
        metadata_path=metadata_path,
        publish=publish,
    )


if __name__ == "__main__":
    app()
