from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.registry import Pull
from dcpy.lifecycle import config
from dcpy.lifecycle.connector_registry import connectors

# todo: make all references to this this dynamic
source_connectors = connectors.get_subregistry(Pull)


def get_source_connectors():
    return connectors.get_subregistry(Pull)


def get_raw_datastore_connector() -> Connector:
    """DCPY_INGEST_RAW_CONNECTOR overrides the default, edm.recipes.raw_datasets."""
    return connectors[
        config.connector_name("ingest", "default_raw_connector"), Connector
    ]


def get_processed_datastore_connector() -> Connector:
    """DCPY_INGEST_DATASETS_CONNECTOR overrides the default, edm.recipes.datasets."""
    return connectors[
        config.connector_name("ingest", "default_datasets_connector"), Connector
    ]
