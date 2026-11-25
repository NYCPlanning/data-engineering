from os import environ as env
from pathlib import Path
from typing import Unpack

from dcpy import configuration
from dcpy.configuration import PRIVATE_BUCKET
from dcpy.connectors import (
    checkbook_nyc,
    filesystem,
    ingest_datastore,
    s3,
    sftp,
    usfws_wetlands,
    web,
)
from dcpy.connectors.edm import gis
from dcpy.connectors.edm.bytes import BytesConnector
from dcpy.connectors.edm.connectors import (
    BuildsConnector,
    DraftsConnector,
    PlanConnector,
    PrivateConnector,
    PublishedConnector,
)
from dcpy.connectors.edm.open_data_nyc import OpenDataConnector
from dcpy.connectors.esri.arcgis_feature_service import ArcGISFeatureServiceConnector
from dcpy.connectors.hybrid_pathed_storage import (
    PathedStorageConnector,
    S3Endpoint,
    S3Kwargs,
    StorageType,
)
from dcpy.connectors.registry import (
    Connector,
    ConnectorRegistry,
)
from dcpy.connectors.socrata.connector import SocrataConnector
from dcpy.connectors.web_scrapers import (
    foodbankny,
    myschools_nyc,
    nycdoc,
    usps,
)
from dcpy.utils.logging import logger

connectors = ConnectorRegistry[Connector]()

SFTP_HOST = env.get("SFTP_HOST")
SFTP_USER = env.get("SFTP_USER")
SFTP_PORT = str(env.get("SFTP_PORT", "22"))
SFTP_PRIVATE_KEY_PATH: Path | None = (
    Path(env["SFTP_PRIVATE_KEY_PATH"]) if env.get("SFTP_PRIVATE_KEY_PATH") else None
)

# Axway SecureTransport - DOF's PLUTO input files (PTS_Propmast.gz, CAMA.zip)
AXWAY_SFTP_HOST = env.get("AXWAY_SFTP_HOST", "securetransport.nyc.gov")
AXWAY_SFTP_USER = env.get("AXWAY_SFTP_USER")
AXWAY_SFTP_PASSWORD = env.get("AXWAY_SFTP_PASSWORD")
AXWAY_SFTP_PORT = str(env.get("AXWAY_SFTP_PORT", "22"))


def _make_ingest_datastores(
    prefix: str = "",
    recipes_bucket: str | None = None,
    private_bucket: str | None = None,
    recipes_root_folder: str | None = None,
    **s3: Unpack[S3Endpoint],
):
    """datasets/ and raw/ sit under `recipes_root_folder` (default RECIPES_BUCKET_ROOT_FOLDER).

    The private connector writes `datasets/` at its bucket's root, unless the private bucket is
    the recipes bucket (Azure has no edm-private), where it shares the recipes root folder.
    """
    recipes_bucket = recipes_bucket or configuration.RECIPES_BUCKET
    assert recipes_bucket
    private_bucket = private_bucket or PRIVATE_BUCKET
    if recipes_root_folder is None:
        recipes_root_folder = configuration.recipes_bucket_root_folder()
    private_root_folder = (
        recipes_root_folder if private_bucket == recipes_bucket else ""
    )

    def storage(conn_type: str, bucket: str, root_folder: str):
        return PathedStorageConnector.from_storage_kwargs(
            conn_type=conn_type,
            storage_backend=StorageType.S3,
            s3_bucket=bucket,
            root_folder=root_folder,
            _validate_root_path=False,
            **s3,
        )

    datasets = f"{prefix}edm.recipes.datasets"
    raw = f"{prefix}edm.recipes.raw_datasets"
    private = f"{prefix}edm.private"
    return [
        [
            ingest_datastore.Connector(
                conn_type=datasets,
                storage=storage(
                    datasets,
                    recipes_bucket,
                    configuration.join_root_folder(recipes_root_folder, "datasets"),
                ),
            ),
            datasets,
        ],
        [
            ingest_datastore.Connector(
                conn_type=raw,
                storage=storage(
                    raw,
                    recipes_bucket,
                    configuration.join_root_folder(recipes_root_folder, "raw"),
                ),
            ),
            raw,
        ],
        [
            PrivateConnector.create(
                storage=storage(
                    private,
                    private_bucket,
                    configuration.join_root_folder(private_root_folder, "datasets"),
                ),
                conn_type=private,
            ),
            private,
        ],
    ]


# Azure Blob, reached through the versitygw S3 gateway. Registered next to the default edm.*
# connectors, as az.*, only when AZ_S3_ENDPOINT and its keys are set, so one process can read
# one store and write the other. Bucket names default to the edm.* ones.
AZURE_PREFIX = "az."


def _azure_s3() -> S3Endpoint | None:
    endpoint = env.get("AZ_S3_ENDPOINT")
    key_id = env.get("AZ_S3_ACCESS_KEY_ID")
    secret = env.get("AZ_S3_SECRET_ACCESS_KEY")
    if not endpoint:
        return None
    if not (key_id and secret):
        logger.warning(
            "AZ_S3_ENDPOINT is set without AZ_S3_ACCESS_KEY_ID and AZ_S3_SECRET_ACCESS_KEY; "
            f"not registering the {AZURE_PREFIX}* connectors."
        )
        return None
    return {
        "s3_endpoint_url": endpoint,
        "s3_access_key_id": key_id,
        "s3_secret_access_key": secret,
    }


def _make_azure_connectors(s3: S3Endpoint) -> list:
    publishing: S3Kwargs = {
        **s3,
        "s3_bucket": env.get("AZ_PUBLISHING_BUCKET")
        or configuration.PUBLISHING_BUCKET
        or "edm-publishing",
    }
    return [
        *_make_ingest_datastores(
            prefix=AZURE_PREFIX,
            recipes_bucket=env.get("AZ_RECIPES_BUCKET"),
            private_bucket=env.get("AZ_PRIVATE_BUCKET"),
            # Unset falls back to RECIPES_BUCKET_ROOT_FOLDER; set it empty for the bucket root.
            recipes_root_folder=env.get("AZ_RECIPES_BUCKET_ROOT_FOLDER"),
            **s3,
        ),
        *[
            [
                factory(conn_type=f"{AZURE_PREFIX}{name}", **publishing),
                f"{AZURE_PREFIX}{name}",
            ]
            for factory, name in [
                (BuildsConnector.create, "edm.publishing.builds"),
                (DraftsConnector.create, "edm.publishing.drafts"),
                (PublishedConnector.create, "edm.publishing.published"),
                (PlanConnector.create, "edm.publishing.plan"),
            ]
        ],
    ]


def _set_default_connectors():
    connectors.clear()
    recipes_datasets, recipes_raw, edm_private = _make_ingest_datastores()
    conns = [
        recipes_datasets,
        recipes_raw,
        edm_private,
        DraftsConnector.create(),
        PublishedConnector.create(),
        BytesConnector(),
        gis.GisDatasetsConnector.create(),
        SocrataConnector(),
        checkbook_nyc.CheckbookNYCConnector(),
        OpenDataConnector(),
        ArcGISFeatureServiceConnector(),
        web.WebConnector(),
        nycdoc.NYCDOCConnector(),
        foodbankny.FoodBankNYConnector(),
        myschools_nyc.MySchoolsNYCConnector(),
        usps.USPSLocationsConnector(),
        usfws_wetlands.USFWSWetlandsConnector(),
        [web.WebConnector(), "api"],
        [filesystem.Connector(), "local_file"],
        [s3.S3Connector(), "s3"],
        [BuildsConnector.create(), "edm.publishing.builds"],
        [PlanConnector.create(), "edm.publishing.plan"],
        [
            sftp.SFTPConnector(
                hostname=SFTP_HOST,
                username=SFTP_USER,
                port=int(SFTP_PORT),
                private_key_path=SFTP_PRIVATE_KEY_PATH,
            ),
            "ginger",  # TODO - name and env var names should be configurable
        ],
        [
            sftp.SFTPConnector(
                hostname=AXWAY_SFTP_HOST,
                username=AXWAY_SFTP_USER,
                port=int(AXWAY_SFTP_PORT),
                password=AXWAY_SFTP_PASSWORD,
            ),
            "axway",  # DOF Axway SecureTransport - PLUTO input files
        ],
    ]

    if azure_s3 := _azure_s3():
        conns.extend(_make_azure_connectors(azure_s3))

    for conn in conns:
        if type(conn) is list:
            connectors.register(conn[0], conn_type=conn[1])
        else:
            connectors.register(conn)

    logger.debug(f"Registered Connectors: {connectors.list_registered()}")


_set_default_connectors()
