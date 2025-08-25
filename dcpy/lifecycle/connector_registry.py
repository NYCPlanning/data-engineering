from dcpy.configuration import (
    RECIPES_BUCKET,
    SFTP_HOST,
    SFTP_USER,
    SFTP_PORT,
    SFTP_KNOWN_HOSTS_KEY_PATH,
    SFTP_PRIVATE_KEY_PATH,
)
from dcpy.connectors.edm import recipes, publishing
from dcpy.connectors.edm.bytes import BytesConnector
from dcpy.connectors.edm.open_data_nyc import OpenDataConnector
from dcpy.connectors.socrata.connector import SocrataConnector
from dcpy.connectors.esri.arcgis_feature_service import ArcGISFeatureServiceConnector
from dcpy.connectors import filesystem, web, s3, ingest_datastore, sftp
from dcpy.connectors.registry import (
    ConnectorRegistry,
    Connector,
)
from dcpy.utils.logging import logger

connectors = ConnectorRegistry[Connector]()


def _set_default_connectors():
    connectors.clear()
    conns = [
        recipes.Connector(),
        publishing.DraftsConnector(),
        publishing.PublishedConnector(),
        BytesConnector(),
        publishing.GisDatasetsConnector(),
        SocrataConnector(),
        OpenDataConnector(),
        ArcGISFeatureServiceConnector(),
        web.WebConnector(),
        [web.WebConnector(), "api"],
        [filesystem.Connector(), "local_file"],
        [s3.S3Connector(), "s3"],
        [publishing.BuildsConnector(), "edm.publishing.builds"],
        sftp.SFTPConnectorAdapter(
            hostname=SFTP_HOST,
            username=SFTP_USER,
            port=int(SFTP_PORT),
            known_hosts_path=SFTP_KNOWN_HOSTS_KEY_PATH,
            private_key_path=SFTP_PRIVATE_KEY_PATH,
        ),
    ]

    for conn in conns:
        if type(conn) is list:
            connectors.register(conn[0], conn_type=conn[1])
        else:
            connectors.register(conn)

    logger.debug(f"Registered Connectors: {connectors.list_registered()}")


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
