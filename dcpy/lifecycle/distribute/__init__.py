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
