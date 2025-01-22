from typing import Unpack

from dcpy.lifecycle.distribute.connectors import (
    DistributionSFTPConnector,
    SocrataPublishConnector,
)


from dcpy.models.lifecycle.distribution import PublisherPushKwargs
from dcpy.models.connectors import ConnectorDispatcher


# Register all default connectors for `lifecycle.distribute`.
# Third parties can similarly register their own connectors,
# so long as the connector implements a ConnectorDispatcher protocol.
dispatcher = ConnectorDispatcher[PublisherPushKwargs, dict]()

dispatcher.register(conn_type="socrata", connector=SocrataPublishConnector())
dispatcher.register(conn_type="sftp", connector=DistributionSFTPConnector())


def from_local(**pub_kwargs: Unpack[PublisherPushKwargs]) -> str:
    """Distribute a dataset and specific dataset_destination_id.

    Requires fully rendered template, ie there should be no template variables in the metadata
    """
    dest = pub_kwargs["metadata"].get_destination(pub_kwargs["dataset_destination_id"])
    dest_type = dest.type
    try:
        return dispatcher.push(dest_type, pub_kwargs)
    except Exception as e:
        return f"Error pushing {pub_kwargs['metadata'].attributes.display_name} to dest_type: {dest_type}, destination: {dest.id}: {str(e)}"
