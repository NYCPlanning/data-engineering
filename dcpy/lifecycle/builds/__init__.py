from typing import Any

from dcpy.connectors.edm import recipes, publishing
from dcpy.models.connectors import VersionedConnectorRegistry
from dcpy.models.lifecycle.builds import InputDataset
from dcpy.utils.logging import logger

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

# Register all default connectors for `lifecycle.build`.
def set_default_connectors() -> VersionedConnectorRegistry:
    conns = VersionedConnectorRegistry()
    conns.register(connector=recipes.Connector())
    conns.register(connector=publishing.DraftsConnector())
    conns.register(connector=publishing.PublishedConnector())
    logger.info(f"Registered Connectors for `lifecycle.build`: {conns.list_registered()}")
    return conns

connectors = set_default_connectors()
