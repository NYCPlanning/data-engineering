from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.registry import Pull
from dcpy.lifecycle.connector_registry import connectors

# todo: make all references to this this dynamic
source_connectors = connectors.get_subregistry(Pull)


def get_source_connectors():
    return connectors.get_subregistry(Pull)


def get_raw_datastore_connector() -> Connector:
    return connectors["edm.recipes.raw_datasets", Connector]


def get_processed_datastore_connector() -> Connector:
    return connectors["edm.recipes.datasets", Connector]
