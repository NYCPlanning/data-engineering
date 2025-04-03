from dcpy.lifecycle.connector_registry import connectors, ingest_datastore
from dcpy.connectors.registry import NonVersionedPull, VersionedPull, VersionedPush

sources = connectors.get_subregistry[Pull]
raw_datastore = connectors["edm.recipes.raw_datasets", VersionedPush]
raw_datastore.push()
processed_datastore = connectors["edm.recipes.datasets"]
