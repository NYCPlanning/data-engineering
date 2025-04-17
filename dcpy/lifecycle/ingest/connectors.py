from dcpy.lifecycle.connector_registry import connectors
from dcpy.connectors.ingest_datastore import Connector
from dcpy.connectors.registry import Pull

source_connectors = connectors.get_subregistry(Pull)
raw_datastore = connectors["edm.recipes.raw_datasets", Connector]
processed_datastore = connectors["edm.recipes.datasets", Connector]
