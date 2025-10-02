from dcpy.lifecycle.builds.connector import get_recipes_default_connector

# TODO: Move these out of here
BUILD_REPO = "data-engineering"
BUILD_DBS = [
    "db-cbbr",
    "db-cdbg",
    "db-ceqr",
    "db-checkbook",
    "db-colp",
    "db-cpdb",
    "db-devdb",
    "db-facilities",
    "db-green-fast-track",
    # "db-lion", we need to preserve schemas while this data product is in development
    "db-pluto",
    "db-template",
    "db-ztl",
    "kpdb",
]

__all__ = ["get_recipes_default_connector"]
