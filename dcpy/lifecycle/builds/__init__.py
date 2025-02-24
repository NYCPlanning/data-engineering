from typing import Any

from dcpy.models.connectors import VersionedConnectorRegistry
from dcpy.models.lifecycle.builds import InputDataset
from dcpy.connectors.edm import recipes

BUILD_REPO = "data-engineering"
BUILD_DBS = [
    "db-cbbr",
    "db-cdbg",
    "db-checkbook",
    "db-colp",
    "db-cpdb",
    "db-devdb",
    "db-facilities",
    "db-green-fast-track",
    "db-pluto",
    "db-template",
    "db-ztl",
    "kpdb",
]


connectors = VersionedConnectorRegistry()


# Register all default connectors for `lifecycle.build`.
def set_default_connectors():
    connectors.register(conn_type="edm.recipes", connector=recipes.Connector())


set_default_connectors()
