from pathlib import Path
from dcpy.lifecycle.distribute.connectors import (
    DistributionSFTPConnector,
    SocrataPublishConnector,
)
from dcpy.models.lifecycle.distribute import DatasetDestinationPushArgs
from dcpy.models.lifecycle.dataset_event import DistributeResult
from dcpy.lifecycle import product_metadata

from dcpy.connectors.registry import ConnectorDispatcher


# Register all default connectors for `lifecycle.distribute`.
# Third parties can similarly register their own connectors,
# so long as the connector implements a ConnectorDispatcher protocol.
dispatcher = ConnectorDispatcher[DatasetDestinationPushArgs, dict]()

dispatcher.register(conn_type="socrata", connector=SocrataPublishConnector())
dispatcher.register(conn_type="sftp", connector=DistributionSFTPConnector())


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
        # TODO: In a followup, replace dispatcher with connector_registry.
        # Need to implement for Socrata and FTP, else I'd just do it here.
        result = dispatcher.push(
            dest_type,
            arg={
                "metadata": ds_md,
                "dataset_destination_id": destination_id,
                "dataset_package_path": package_path,
                "publish": publish,
                "metadata_only": metadata_only,
            },
        )
        return dist_result(
            success=True,
            result_summary="Distribution succeeded",
            result_details=result,
        )
    except Exception as e:
        return dist_result(
            success=False,
            result_summary="Distribution Failed",
            result_details=str(e),
        )
