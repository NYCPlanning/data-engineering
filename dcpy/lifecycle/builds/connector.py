from dcpy.lifecycle import config
from dcpy.lifecycle.connector_registry import connectors

LIFECYCLE_STAGE = "builds"


# Putting this in a separate file so that sub-stages can access it,
# but so that we can also expose this in the __init__
def get_recipes_default_connector():
    return connectors.versioned[
        config.stage_config(LIFECYCLE_STAGE)["default_recipes_connector"]
    ]


def get_builds_default_connector():
    return connectors.versioned[
        config.stage_config(LIFECYCLE_STAGE)["default_builds_connector"]
    ]


def get_drafts_default_connector():
    return connectors.versioned[
        config.stage_config(LIFECYCLE_STAGE)["default_drafts_connector"]
    ]


def get_published_default_connector():
    return connectors.versioned[
        config.stage_config(LIFECYCLE_STAGE)["default_published_connector"]
    ]
