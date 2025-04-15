from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata.connector import SocrataConnector
from dcpy.connectors.esri.arcgis_feature_service import ArcGISFeatureServiceConnector
from dcpy.connectors import web
from dcpy.connectors.registry import (
    ConnectorRegistry,
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
    connectors.register(connector=SocrataConnector())
    connectors.register(connector=ArcGISFeatureServiceConnector())
    connectors.register(connector=web.WebConnector())
    connectors.register(connector=web.WebConnector(), conn_type="api")
    logger.info(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
