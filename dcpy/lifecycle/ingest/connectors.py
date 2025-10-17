from dcpy.lifecycle import connector_registry
from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.registry import Pull

# todo: make all references to this this dynamic
source_connectors = connector_registry.connectors.get_subregistry(Pull)


def get_source_connectors():
    return connector_registry.connectors.get_subregistry(Pull)


def get_raw_datastore_connector() -> Connector:
    return connector_registry.connectors["edm.recipes.raw_datasets", Connector]


def get_processed_datastore_connector() -> Connector:
    return connector_registry.connectors["edm.recipes.datasets", Connector]
