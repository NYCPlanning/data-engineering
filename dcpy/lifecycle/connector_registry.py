from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata import connector as soc_connector
from dcpy.connectors.esri import arcgis_feature_service
from dcpy.connectors import web
from dcpy.connectors.registry import (
    ConnectorRegistry,
    NonVersionedConnector,
    Connector,
)
from dcpy.utils.logging import logger

connectors = ConnectorRegistry[Connector]()


def _set_default_connectors():
    connectors.clear()
    connectors.register(connector=recipes.Connector())
    connectors.register(connector=publishing.DraftsConnector())
    connectors.register(connector=publishing.PublishedConnector())
    connectors.register(connector=publishing.GisDatasetsConnector())
    connectors.register(connector=soc_connector.Connector())
    connectors.register(connector=arcgis_feature_service.Connector())
    connectors.register(connector=web.Connector())
    connectors.register(connector=web.Connector(), conn_type="api")
    logger.info(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
