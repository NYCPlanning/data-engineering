from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.registry import VersionedConnectorRegistry
from dcpy.utils.logging import logger

connectors = VersionedConnectorRegistry()


def _set_default_connectors():
    connectors.clear()
    connectors.register(connector=recipes.Connector())
    connectors.register(connector=publishing.DraftsConnector())
    connectors.register(connector=publishing.PublishedConnector())
    logger.info(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
