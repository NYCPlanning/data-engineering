from pathlib import Path

from dcpy.lifecycle import product_metadata
from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle.models import DistributeResult
from dcpy.product_metadata.keys import DestinationKey
from dcpy.utils.logging import logger


def distribute_build(
    build_path: Path,
    destination_key: str,
    version: str,
    *,
    publish: bool = False,
    metadata_only: bool = False,
) -> DistributeResult:
    """Distribute a build to a destination using a full destination key.

    Args:
        build_path: Path to the build output directory
        destination_key: Full destination key in format {product}.{dataset}.{destination}
        version: Build version
        publish: Whether to publish the distribution (e.g., Socrata revisions)
        metadata_only: Only push metadata (including attachments), not data files

    Returns:
        DistributeResult with success status and details
    """
    # Parse destination key
    try:
        dest_key = DestinationKey.from_path_str(destination_key)
    except (ValueError, IndexError) as e:
        logger.error(f"Invalid destination key format: {destination_key}")
        return DistributeResult(
            product="",
            dataset="",
            version=version,
            destination_id=destination_key,
            local_package_path=build_path,
            success=False,
            result_summary="Invalid destination key format",
            result_details=str(e),
        )

    # Use the existing to_dataset_destination function
    return to_dataset_destination(
        product=dest_key.product,
        dataset=dest_key.dataset,
        version=version,
        destination_id=dest_key.destination_id,
        package_path=build_path,
        publish=publish,
        metadata_only=metadata_only,
    )


def to_dataset_destination(
    product: str,
    dataset: str,
    version: str,
    destination_id: str,
    package_path: Path,
    *,
    publish,
    metadata_only,
) -> DistributeResult:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    org_md = product_metadata.load()
    ds_md = org_md.product(product).dataset(dataset)
    dest = ds_md.get_destination(destination_id)
    dest_type = dest.type

    def dist_result(**remaining_kwargs):
        return DistributeResult(
            product=product,
            dataset=dataset,
            version=version,
            destination_id=destination_id,
            local_package_path=package_path,
            **remaining_kwargs,
        )

    try:
        conn = connectors.push[dest_type]

        result = conn.push(
            key=f"{product}.{dataset}.{destination_id}",
            version=version,
            dataset_package_path=package_path,
            publish=publish,
            metadata_only=metadata_only,
        )
        return dist_result(
            success=True,
            result_summary="Distribution succeeded",
            result_details=str(result),
        )
    except Exception as e:
        return dist_result(
            success=False,
            result_summary="Distribution Failed",
            result_details=str(e),
        )
