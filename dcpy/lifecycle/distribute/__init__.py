from pathlib import Path

from dcpy.lifecycle import product_metadata
from dcpy.lifecycle.connector_registry import connectors
from dcpy.models.lifecycle.event_result import DistributeResult


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
    org_md = product_metadata.load(version=version)
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
