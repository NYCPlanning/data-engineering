from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle import config

LIFECYCLE_STAGE = "builds"


# Putting this in a separate file so that sub-stages can access it,
# but so that we can also expose this in the __init__
def get_recipes_default_connector():
    return connectors.versioned[
        config.stage_config(LIFECYCLE_STAGE)["default_recipes_connector"]
    ]
