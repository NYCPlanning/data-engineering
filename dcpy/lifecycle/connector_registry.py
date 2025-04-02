from dcpy.configuration import RECIPES_BUCKET
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata import connector as soc_connector
from dcpy.connectors.esri import arcgis_feature_service
from dcpy.connectors import drive, web, s3, ingest_datastore
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
    connectors.register(connector=soc_connector.Connector())
    connectors.register(connector=arcgis_feature_service.Connector())
    connectors.register(connector=web.Connector())
    connectors.register(connector=web.Connector(), conn_type="api")
    connectors.register(connector=drive.Connector(), conn_type="local_file")
    connectors.register(connector=s3.Connector(), conn_type="s3")
    connectors.register(
        connector=s3.Connector(bucket=RECIPES_BUCKET, prefix="datasets/"),
        conn_type="s3_edm.recipes.datasets",
    )
    connectors.register(
        connector=s3.Connector(bucket=RECIPES_BUCKET, prefix="raw_datasets/"),
        conn_type="s3_edm.recipes.raw_datasets",
    )
    connectors.register(
        ingest_datastore.Connector(
            storage_type="s3_edm.recipes.datasets", registry=connectors
        ),
        conn_type="edm.recipes.datasets",
    )
    connectors.register(
        ingest_datastore.Connector(
            storage_type="s3_edm.recipes.raw_datasets", registry=connectors
        ),
        conn_type="edm.recipes.raw_datasets",
    )
    logger.info(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
