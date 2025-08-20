from itertools import groupby
import typer

from dcpy.lifecycle import package, distribute, product_metadata
from dcpy.models.lifecycle import dataset_event
from dcpy.utils.logging import logger


def get_destinations_by_product_dataset_and_type(
    prod_ds_dest_filters, org_md
) -> list[list[str]]:
    dest_paths = org_md.query_product_dataset_destinations(**prod_ds_dest_filters)

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
    prod_ds_dest_filters,
    source_id,
    metadata_only=False,
    publish=False,
    validate_dataset_files=False,
    max_destinations=50,
    dry_run=False,
) -> list[dataset_event.DistributeResult]:
    """Package and Distribute to dataset destinations.

    Destinations are batched by product, dataset, and destination type.
    Files for the dataset are assembled, then distributed to the destinations.
    """
    org_md = product_metadata.load(version=version)
    destinations = get_destinations_by_product_dataset_and_type(
        prod_ds_dest_filters, org_md
    )

    # Sanity check that we're not accidently distributing way to much (ie. every destination)
    total_destinations = len(sum(destinations, []))
    logger.info(f"Found destinations: {destinations}")
    assert total_destinations > 0, "Found no destinations for provided filters"
    assert total_destinations <= max_destinations, (
        f"Filters returned {total_destinations} destinations, which exceeds max allowed ({max_destinations})"
    )

    results: list[dataset_event.DistributeResult] = []
    for batch in destinations:
        product, dataset, _ = batch[0].split(".")
        if dry_run:
            results += [
                dataset_event.DistributeResult(
                    product=product,
                    dataset=dataset,
                    version=version,
                    success=True,
                    destination_id=b.split(".")[2],
                    result_summary="Dry Run",
                    result_details="Nothing to report.",
                )
                for b in batch
            ]
            continue
        logger.info(
            f"Beginning Package + Assembling files for destinations: {', '.join(batch)}"
        )
        package_result = package.assemble_dataset_package(
            source_id=source_id,
            product=product,
            dataset=dataset,
            version=version,
            validate_dataset_files=validate_dataset_files,
        )
        if not package_result.success:
            result = dataset_event.DistributeResult(
                destination_id=",".join(batch), **package_result.model_dump()
            )
            results.append(result)
            continue

        assert package_result.package_path
        results += [
            distribute.to_dataset_destination(
                product=product,
                dataset=dataset,
                destination_id=b.split(".")[2],
                version=version,
                package_path=package_result.package_path,
                metadata_only=metadata_only,
                publish=publish,
            )
            for b in batch
        ]

    return results


app = typer.Typer()


@app.command("product")
def package_and_distribute_product(
    product: str,
    version: str,
    source: str,
    datasets: list[str] = typer.Option(
        [],
        "-d",
        "--datasets",
        help="List of Datasets",
    ),
    destination_tags: list[str] = typer.Option(
        [],
        "-t",
        "--dest-tag",
        help="Destination tag to filter for.",
    ),
    destination_types: list[str] = typer.Option(
        [],
        "-y",
        "--dest-types",
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
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Perform a dry run (will just list destinations).",
    ),
):
    results = run(
        version,
        source_id=source,
        prod_ds_dest_filters={
            "product_ids": {product},
            "dataset_ids": set(datasets),
            "destination_filter": {
                "tags": set(destination_tags),
                "types": set(destination_types),
            },
        },
        metadata_only=metadata_only,
        publish=publish,
        validate_dataset_files=validate_dataset_files,
        dry_run=dry_run,
    )

    if any([not r.success for r in results]):
        logger.error(
            f"Distribution for {product}.{version} finished, but issues occurred!"
        )
    else:
        logger.info("Finished distributing batches with no errors.")

    typer.echo(dataset_event.make_results_table(results))
