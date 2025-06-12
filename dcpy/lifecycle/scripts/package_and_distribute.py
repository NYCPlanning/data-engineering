from itertools import groupby
from pathlib import Path
from tabulate import tabulate  # type: ignore
import typer

from dcpy import configuration
from dcpy.models.product import metadata as product_metadata
from dcpy.models.lifecycle.distribute import (
    DistributeResult,
    DatasetDestination,
    DatasetDestinationPushArgs,
)
from dcpy.models.lifecycle.validate import ValidationArgs
import dcpy.models.product.metadata as prod_md
from dcpy.lifecycle import distribute, package
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


def _get_product_metadata(version: str, *, md_path_override: Path | None = None):
    md_path = md_path_override or configuration.PRODUCT_METADATA_REPO_PATH
    assert md_path
    return prod_md.OrgMetadata.from_path(
        Path(md_path),
        template_vars={"version": version},
    )


def package_and_distribute_destinations(
    org_md: product_metadata.OrgMetadata,
    destinations: list[DatasetDestination],  # TODO: add version to DatasetDestination
    *,
    distribution_config: DatasetDestinationPushArgs,
    validation_conf: ValidationArgs = {},
) -> list[DistributeResult]:
    """Package and distribute specified destinations."""

    # Package
    destination_packages: dict[str, PackagePullResult] = {}
    for dest in destinations:
        destination_packages[dest["destination_path"]] = package.pull(dest, org_md), validation_config=validation_conf)

    # Distribute
    distribute_results: list[distribute.DistributeResult] = []
    for dest in destinations:
        logger.info(f"Distributing {dataset}-{dest_id} from {package_path}")
        package = destination_packages[dest["destination_path"]]
        result = distribute.to_dataset_destination(
            package_path=package.path
            distribution_config=distribution_config
            org_md=org_md
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


def package_and_distribute(product_md, **destination_filters):
    """Looking forward, I think we'd probably want to execute individual"""
    dests = product_md.query_dataset_destinations(**destination_filters)
    grouped_by_dataset_and_type = [
        [k, list(v)]
        for k, v in groupby(
            dests, lambda d: f"{d['dataset_id']}-{d['destination_type']}"
        )
    ]
    results = []
    for batch, dataset_destinations in grouped_by_dataset_and_type:
        logger.info(
            f"distributing {len(dataset_destinations)} destinations for batch: {batch}"
        )
        results.append(package_and_distribute_destinations())
    return results


app = typer.Typer()


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
