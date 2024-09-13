from os import environ as env

CI = "CI" in env

BUILD_NAME = env.get("BUILD_NAME")

DEV_FLAG = env.get("DEV_FLAG") == "true"

RECIPES_BUCKET = env.get("RECIPES_BUCKET")
PUBLISHING_BUCKET = env.get("PUBLISHING_BUCKET")

LOGGING_DB = "edm-qaqc"
LOGGING_SCHEMA = "product_data"
LOGGING_TABLE_NAME = "event_logging"
PRODUCTS_TO_LOG = [
    "db-cbbr",
    "db-colp",
    "db-cpdb",
    "db-developments",
    "db-facilities",
    "db-green-fast-track",
    "db-pluto",
    "db-template",
    "db-zoningtaxlots",
]
IGNORED_LOGGING_BUILDS = ["nightly_qa"]
