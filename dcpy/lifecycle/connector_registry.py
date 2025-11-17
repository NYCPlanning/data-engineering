from dcpy import configuration
from dcpy.configuration import (
    SFTP_HOST,
    SFTP_USER,
    SFTP_PORT,
    SFTP_PRIVATE_KEY_PATH,
)
from dcpy.connectors.edm import publishing
from dcpy.connectors.edm.bytes import BytesConnector
from dcpy.connectors.edm.open_data_nyc import OpenDataConnector
from dcpy.connectors.hybrid_pathed_storage import (
    StorageType,
    PathedStorageConnector,
)
from dcpy.connectors.socrata.connector import SocrataConnector
from dcpy.connectors.esri.arcgis_feature_service import ArcGISFeatureServiceConnector
from dcpy.connectors import filesystem, web, s3, ingest_datastore, sftp, overture
from dcpy.connectors.registry import (
    ConnectorRegistry,
    Connector,
)
from dcpy.utils.logging import logger

connectors = ConnectorRegistry[Connector]()


def _make_ingest_datastores():
    assert configuration.RECIPES_BUCKET
    return [
        [
            ingest_datastore.Connector(
                storage=PathedStorageConnector.from_storage_kwargs(
                    conn_type="edm.recipes.datasets",
                    storage_backend=StorageType.S3,
                    s3_bucket=configuration.RECIPES_BUCKET,
                    root_folder="datasets",
                    _validate_root_path=False,
                )
            ),
            "edm.recipes.datasets",
        ],
        [
            ingest_datastore.Connector(
                storage=PathedStorageConnector.from_storage_kwargs(
                    conn_type="edm.recipes.raw_datasets",
                    storage_backend=StorageType.S3,
                    s3_bucket=configuration.RECIPES_BUCKET,
                    root_folder="raw",
                    _validate_root_path=False,
                )
            ),
            "edm.recipes.raw_datasets",
        ],
    ]


def _set_default_connectors():
    connectors.clear()
    recipes_datasets, recipes_raw = _make_ingest_datastores()
    conns = [
        recipes_datasets,
        recipes_raw,
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
        [
            sftp.SFTPConnector(
                hostname=SFTP_HOST,
                username=SFTP_USER,
                port=int(SFTP_PORT),
                private_key_path=SFTP_PRIVATE_KEY_PATH,
            ),
            "ginger",  # TODO - name and env var names should be configurable
        ],
        overture.OvertureConnector(),
    ]

    for conn in conns:
        if type(conn) is list:
            connectors.register(conn[0], conn_type=conn[1])
        else:
            connectors.register(conn)

    logger.debug(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
