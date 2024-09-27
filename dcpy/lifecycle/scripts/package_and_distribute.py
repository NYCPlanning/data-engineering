from pathlib import Path
import typer

from dcpy.models.product import metadata as product_metadata
from dcpy.lifecycle.distribute import socrata as soc_dist
from dcpy.lifecycle.package import assemble
from dcpy.utils.logging import logger


def from_bytes_to_tagged_socrata(
    product_metadata_path: Path,
    version: str,
    destination_tag: str,
    source_destination_id: str = "bytes",
    **publish_kwargs,
):
    """Package from bytes, and"""
    f = product_metadata.ProductFolder(
        root_path=product_metadata_path,
        template_vars={"version": version},
    )
    dests = f.get_tagged_destinations(destination_tag)
    product = f.get_product_metadata()

    logger.info(f"Packaging {product.id}. Datasets: {list(dests.keys())}")
    package_paths = {}
    for ds_id, dests_to_mds in dests.items():
        dataset_metadata = list(dests_to_mds.values())[0]
        out_path = assemble.assemble_dataset_from_bytes(
            dataset_metadata,
            product=product.id,
            version=version,
            source_destination_id=source_destination_id,
        )
        package_paths[ds_id] = out_path

    logger.info("\nFinished Packaging. Beginning Distribution.")
    results = []
    for ds_id, dests_to_mds in dests.items():
        package_path = package_paths[ds_id]

        for dest, _ in dests_to_mds.items():
            logger.info(f"Distributing {ds_id}: {dest} from {package_path}")
            result = soc_dist.dist_from_local(package_path, dest, **publish_kwargs)
            results.append(result)
    return results


app = typer.Typer()


@app.command("from_bytes_to_tagged_socrata")
def from_bytes_to_tagged_socrata_cli(
    product_metadata_path: Path,
    version: str,
    destination_tag: str = typer.Option(
        None,
        "-t",
        "--dest-tag",
        help="Destination tag to package and distribute",
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
    results = from_bytes_to_tagged_socrata(
        product_metadata_path,
        version,
        destination_tag,
        publish=publish,
        ignore_validation_errors=ignore_validation_errors,
        skip_validation=skip_validation,
        metadata_only=metadata_only,
    )
    for r in results:
        print(r)
