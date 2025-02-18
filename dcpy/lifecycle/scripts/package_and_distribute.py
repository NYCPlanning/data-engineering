from pathlib import Path
from tabulate import tabulate  # type: ignore
import typer

from dcpy import configuration
from dcpy.models.product import metadata as product_metadata
from dcpy.models.lifecycle.distribute import DatasetDestinationFilters
from dcpy.models.lifecycle.validate import ValidationArgs
from dcpy.lifecycle import package
from dcpy.lifecycle import distribute
from dcpy.utils.logging import logger


# Future thought: this type of glue function shouldn't be necessary if
# we can string together lifecycle stages a little more declaratively
def package_and_distribute(
    org_md: product_metadata.OrgMetadata,
    product: str,
    dataset: str,
    version: str,
    *,
    source_destination_id: str,
    publish: bool = False,
    metadata_only: bool = False,
    validation_conf: ValidationArgs = {},
    destination_filters: DatasetDestinationFilters = {},
):
    """Package tagged datasets from the source, and distribute to specified destinations."""

    logger.info(f"Packaging and Distributing with filters: {destination_filters}")

    destination_filters["datasets"] = frozenset({dataset})
    product_md = org_md.product(product)
    dataset_md = product_md.dataset(dataset)
    destinations = product_md.query_destinations(**destination_filters)[dataset]
    assert destinations, "No Destinations found! Check your destination filters"

    logger.info(f"Target destinations are {list(destinations.keys())}")
    package_path = package.ASSEMBLY_DIR / product / version / dataset

    package.pull_destination_package_files(
        local_package_path=package_path,
        source_destination_id=source_destination_id,
        dataset_metadata=dataset_md,
    )

    package.assemble_package(
        org_md=org_md,
        product=product,
        dataset=dataset,
        version=version,
        source_destination_id=source_destination_id,
        metadata_only=metadata_only,
    )

    # validate
    if not (validation_conf.get("skip_validation") or metadata_only):
        package_validations = package.validate_package(package_path=package_path)
        if package_validations.get_errors_list():
            if validation_conf.get("ignore_validation_errors"):
                logger.warning("Package Errors Found! But continuing distribute")
                logger.warning(package_validations.make_errors_table())
            else:
                logger.error("Errors Found! Aborting distribute")
                logger.error(package_validations.make_errors_table())
                raise Exception(package_validations.make_errors_table())
        else:
            logger.info("\nFinished Packaging. Beginning Distribution.")

    # distribute
    # TODO: pull this logic into package.distribute
    distribute_results: list[distribute.DistributeResult] = []
    for dest_id, dest_ds_md in destinations.items():
        logger.info(f"Distributing {dataset}-{dest_id} from {package_path}")
        result = distribute.to_dataset_destination(
            metadata=dest_ds_md,
            dataset_destination_id=dest_id,
            publish=publish,
            dataset_package_path=package_path,
            metadata_only=metadata_only,
        )
        distribute_results.append(result)

    stringified_results = tabulate(
        [
            [r.dataset_id, r.destination_id, r.destination_type, r.success, r.result]
            for r in distribute_results
        ],
        headers=["dataset", "destination_id", "destination type", "success?", "result"],
        tablefmt="presto",
        maxcolwidths=[20, 10, 10, 10, 50],
    )

    if any([not r.success for r in distribute_results]):
        logger.error("Distribution Finished, but issues occurred!")
        logger.error(stringified_results)
        raise Exception("Distribution errors occurred. See the logs for details.")

    logger.info("Distribution Finished")
    logger.info(stringified_results)


def package_and_distribute_cli(
    product: str,
    version: str,
    dataset: str,
    source_destination_id: str,
    # Filters
    destination_tag: str = typer.Option(
        None,
        "-t",
        "--dest-tag",
        help="Destination tag to filter for.",
    ),
    destination_id: str = typer.Option(
        None,
        "-a",
        "--dest-id",
        help="Destination ID",
    ),
    destination_type: str = typer.Option(
        None,
        "-e",
        "--dest-type",
        help="Destination type for filter for. e.g. 'Socrata'",
    ),
    # Overrides
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
        "-m",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    assert configuration.PRODUCT_METADATA_REPO_PATH
    org_md = product_metadata.OrgMetadata.from_path(
        path=Path(configuration.PRODUCT_METADATA_REPO_PATH),
        template_vars={"version": version},
    )
    package_and_distribute(
        org_md,
        product,
        dataset,
        version,
        source_destination_id=source_destination_id,
        publish=publish,
        metadata_only=metadata_only,
        destination_filters={
            "destination_id": destination_id,
            "destination_type": destination_type,
            "destination_tag": destination_tag,
        },
        validation_conf={
            "ignore_validation_errors": ignore_validation_errors,
            "skip_validation": skip_validation,
        },
    )
