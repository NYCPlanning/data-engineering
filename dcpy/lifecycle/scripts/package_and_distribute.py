from pathlib import Path
from tabulate import tabulate  # type: ignore
import typer

from dcpy import configuration
from dcpy.models.product import metadata as product_metadata
from dcpy.models.lifecycle.distribute import DatasetDestinationFilters, DistributeResult
from dcpy.models.lifecycle.validate import ValidationArgs
import dcpy.models.product.metadata as prod_md
from dcpy.lifecycle import package
from dcpy.lifecycle import distribute
from dcpy.utils.logging import logger


def _make_results_table(distribute_results: list[DistributeResult]) -> str:
    return tabulate(
        [
            [
                r.dataset_id,
                r.destination_id,
                r.destination_type,
                "✅" if r.success else "❌",
                r.result,
            ]
            for r in distribute_results
        ],
        headers=["dataset", "destination_id", "destination type", "success?", "result"],
        tablefmt="presto",
        maxcolwidths=[20, 10, 10, 8, 70],
    )


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
) -> list[DistributeResult]:
    """Package tagged datasets from the source, and distribute to specified destinations."""
    logger.info(
        f"Packaging and Distributing with filters: {destination_filters}. Validation conf: {validation_conf}"
    )

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
        logger.info(str(result)) if result.success else logger.error(str(result))

    if any([not r.success for r in distribute_results]):
        logger.error(
            f"Distribution for {product}.{version}.{dataset}. Finished, but issues occurred!"
        )
        logger.error(str(distribute_results))
    else:
        logger.info("Finished distributing batch. No Errors.")

    return distribute_results


def _get_product_metadata(version: str, *, md_path_override: Path | None = None):
    md_path = md_path_override or configuration.PRODUCT_METADATA_REPO_PATH
    assert md_path
    return prod_md.OrgMetadata.from_path(
        Path(md_path),
        template_vars={"version": version},
    )


app = typer.Typer()


def calculate_tagged_destinations(version: str, product_id: str, tag: str):
    """calculate tagged destination by product for given version.

    This should be sufficient information to deploy all datasets within a product,
    if they're tagged.
    """
    org_md = _get_product_metadata(version)
    product = org_md.product(product_id).query_destinations(destination_tag=tag)

    targets = []
    for ds_id, ds_id_to_md in product.items():
        for dest_id, md in ds_id_to_md.items():
            packaged_files_source = md.get_destination(dest_id).custom.get(
                "packaged_files_source"
            )
            targets.append(
                {
                    "product": product_id,
                    "version": version,
                    "dataset": ds_id,
                    "source_destination_id": packaged_files_source,
                    "destination_id": dest_id,
                }
            )
    return targets


@app.command("dataset")
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
    distribute_results = package_and_distribute(
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
    typer.echo(_make_results_table(distribute_results))


@app.command("product")
def package_and_distribute_product(
    product: str,
    version: str,
    destination_tag: str = typer.Option(
        None,
        "-t",
        "--dest-tag",
        help="Destination tag to filter for.",
    ),
    # Overrides
    publish: bool = typer.Option(
        False,
        "-p",
        "--publish",
        help="Publish the Socrata Revision? Or leave it open.",
    ),
    # TODO: validation seems a little broken... at some point, let's fix this
    # ignore_validation_errors: bool = typer.Option(
    #     False,
    #     "-i",
    #     "--ignore-validation-errors",
    #     help="Ignore Validation Errors? Will still perform validation, but ignore errors, allowing a push",
    # ),
    # skip_validation: bool = typer.Option(
    #     True,
    #     "-y",  # -y(olo)
    #     "--skip-validation",
    #     help="Skip Validation Altogether",
    # ),
    metadata_only: bool = typer.Option(
        False,
        "-m",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
):
    targets = calculate_tagged_destinations(
        version=version, product_id=product, tag=destination_tag
    )
    assert targets, f"No targets found for {version} {product} {destination_tag}"

    prettified_targets = [
        f"{t['product']}.{t['version']}.{t['dataset']}.{t['destination_id']}"
        for t in targets
    ]

    typer.echo("Distributing the following datasets")
    typer.echo("\n".join(prettified_targets))

    distribute_results = []
    for t in targets:
        t["publish"] = publish
        t["metadata_only"] = metadata_only

        destination_id = t["destination_id"]
        t["destination_filters"] = {"destination_id": destination_id}
        del t["destination_id"]

        t["validation_conf"] = {
            "skip_validation": True,
        }

        try:
            distribute_results += package_and_distribute(
                org_md=_get_product_metadata(version), **t
            )
        except Exception as e:
            distribute_results.append(
                DistributeResult(
                    dataset_id=t["dataset"],
                    destination_id=destination_id,
                    destination_type="",
                    result=str(e),
                    success=False,
                )
            )

    typer.echo(_make_results_table(distribute_results))
