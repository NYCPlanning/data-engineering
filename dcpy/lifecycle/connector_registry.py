from dcpy.configuration import RECIPES_BUCKET
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.socrata.connector import SocrataConnector
from dcpy.connectors.esri.arcgis_feature_service import ArcGISFeatureServiceConnector
from dcpy.connectors import filesystem, web, s3, ingest_datastore
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
    connectors.register(connector=filesystem.Connector(), conn_type="local_file")
    connectors.register(connector=s3.S3Connector(), conn_type="s3")
    logger.info(f"Registered Connectors: {connectors.list_registered()}")


def _register_ingest_datastores():
    """TODO - this should ideally happen from config"""
    recipes_datasets_s3 = s3.S3Connector(bucket=RECIPES_BUCKET, prefix="datasets/")
    recipes_raw_datasets_s3 = s3.S3Connector(
        bucket=RECIPES_BUCKET, prefix="raw_datasets/"
    )
    connectors.register(
        ingest_datastore.Connector(storage=recipes_datasets_s3),
        conn_type="edm.recipes.datasets",
    )
    connectors.register(
        ingest_datastore.Connector(storage=recipes_raw_datasets_s3),
        conn_type="edm.recipes.raw_datasets",
    )


_set_default_connectors()
_register_ingest_datastores()
