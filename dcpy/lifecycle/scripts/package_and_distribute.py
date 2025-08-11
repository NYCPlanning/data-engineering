from itertools import groupby
from tabulate import tabulate  # type: ignore
import typer

from dcpy.lifecycle import package, distribute, product_metadata
from dcpy.models.lifecycle.distribute import DistributeResult
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


def get_destinations_by_product_dataset_and_type(
    destination_filters, org_md
) -> list[list[str]]:
    dest_paths = org_md.query_product_dataset_destinations(**destination_filters)

    dests_with_type = [
        {
            "path": p,
            "type": org_md.get_product_dataset_destinations(p).type,
            "product": p.split(".")[0],
            "dataset": p.split(".")[1],
        }
        for p in dest_paths
    ]

    grouped_by_product_dataset_and_type = [
        list([d["path"] for d in dests])
        for _, dests in groupby(
            dests_with_type, lambda d: f"{d['product']}-{d['dataset']}-{d['type']}"
        )
    ]
    return grouped_by_product_dataset_and_type


def run(
    version,
    *,
    destination_filters,
    metadata_only=False,
    publish=False,
    validate_dataset_files=False,
    MAX_DESTINATIONS=50,
):
    """Package and Distribute to dataset destinations.

    Destinations are batched by product, dataset, and destination type.
    Files for the dataset are assembled, then distributed to the destinations.
    """
    org_md = product_metadata.load(version=version)
    results = []
    destinations = get_destinations_by_product_dataset_and_type(
        destination_filters, org_md
    )

    # Sanity check that we're not accidently distributing way to much (ie. every destination)
    total_destinations = len(sum(destinations, []))
    assert total_destinations <= MAX_DESTINATIONS, (
        f"Filters returned {total_destinations} destinations, which exceeds max allowed ({MAX_DESTINATIONS})"
    )

    for batch in destinations:
        product, dataset, _ = batch[0].split(".")
        logger.info(f"distributing destinations for {product}-{dataset}: {batch}")
        package_path, err = package.assemble_dataset_package(
            source_id="bytes",
            product=product,
            dataset=dataset,
            version=version,
            validate_dataset_files=validate_dataset_files,
        )
        if err:
            results.append(err)
            continue

        results += [
            distribute.to_dataset_destination(
                metadata=org_md.product(product).dataset(dataset),
                dataset_destination_id=dest,
                dataset_package_path=package_path,
                metadata_only=metadata_only,
                publish=publish,
            )
            for dest in batch
        ]

    return results


app = typer.Typer()


@app.command("product")
def package_and_distribute_product(
    product: str,
    version: str,
    datasets: list[str] = typer.Option(
        None,
        "-d",
        "--datasets",
        help="List of Datasets",
    ),
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
    metadata_only: bool = typer.Option(
        False,
        "-m",
        "--metadata-only",
        help="Only push metadata (including attachments).",
    ),
    validate_dataset_files: bool = typer.Option(
        False,
        "-v",
        "--validate",
        help="Validate assembled dataset files?",
    ),
):
    results = run(
        version,
        destination_filters={"product_ids": {product}, "dataset_ids": set(datasets)},
        metadata_only=False,
        publish=False,
        validate_dataset_files=not validate_dataset_files,
    )

    if any([not r.success for r in results]):
        logger.error(
            f"Distribution for {product}.{version}.{datasets}. Finished, but issues occurred!"
        )
        logger.error(str(results))
    else:
        logger.info("Finished distributing batch. No Errors.")

    _make_results_table(results)
