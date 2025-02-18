from typing import Any

from dcpy.models.connectors import ConnectorDispatcher
from dcpy.models.lifecycle.builds import InputDataset
from dcpy.connectors.edm import recipes
from dcpy.connectors.edm.publishing import RecipeConnectorAdapter

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

# Create a dispatcher that which takes an InputDataset as it's pull arg
dispatcher = ConnectorDispatcher[Any, InputDataset]()

# Register all default connectors for `lifecycle.build`.
dispatcher.register(conn_type="edm.recipes", connector=recipes.Connector())
dispatcher.register(conn_type="edm.publishing", connector=RecipeConnectorAdapter())
