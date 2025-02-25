from typing import Unpack

from dcpy.lifecycle.distribute.connectors import (
    DistributionSFTPConnector,
    SocrataPublishConnector,
)
from dcpy.models.lifecycle.distribute import (
    DatasetDestinationPushArgs,
    DistributeResult,
)
from dcpy.connectors.registry import ConnectorDispatcher


# Register all default connectors for `lifecycle.distribute`.
# Third parties can similarly register their own connectors,
# so long as the connector implements a ConnectorDispatcher protocol.
dispatcher = ConnectorDispatcher[DatasetDestinationPushArgs, dict]()

dispatcher.register(conn_type="socrata", connector=SocrataPublishConnector())
dispatcher.register(conn_type="sftp", connector=DistributionSFTPConnector())


def to_dataset_destination(
    **push_kwargs: Unpack[DatasetDestinationPushArgs],
) -> DistributeResult:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    ds_md = push_kwargs["metadata"]
    dest = ds_md.get_destination(push_kwargs["dataset_destination_id"])
    dest_type = dest.type

    try:
        result = dispatcher.push(dest_type, push_kwargs)
        return DistributeResult.from_push_kwargs(
            result=result, success=True, push_args=push_kwargs
        )
    except Exception as e:
        return DistributeResult.from_push_kwargs(
            result=f"Error pushing {push_kwargs['metadata'].attributes.display_name} to dest_type: {dest_type}, destination: {dest.id}: {str(e)}",
            success=False,
            push_args=push_kwargs,
        )
