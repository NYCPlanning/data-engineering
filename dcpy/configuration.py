from os import environ as env
from pathlib import Path

CI = "CI" in env

BUILD_NAME = env.get("BUILD_NAME")

DEV_FLAG = env.get("DEV_FLAG") == "true"

# Defaulting this is maybe not ideal. However, it does enable us ensure
# that we're NOT using the default bucket in integration tests.
# This should also probably eventually live in pyproject.
_DEFAULT_RECIPES_BUCKET = "edm-recipes"
RECIPES_BUCKET = env.get("RECIPES_BUCKET", _DEFAULT_RECIPES_BUCKET)

DEFAULT_S3_URL = "https://nyc3.digitaloceanspaces.com"

PUBLISHING_BUCKET = env.get("PUBLISHING_BUCKET")
PUBLISHING_BUCKET_ROOT_FOLDER: str = env.get("PUBLISHING_BUCKET_ROOT_FOLDER", "")

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
IGNORED_LOGGING_BUILDS = ["nightly_qa", "compile_python_reqs"]

PRODUCT_METADATA_REPO_PATH = env.get("PRODUCT_METADATA_REPO_PATH")

INGEST_DEF_DIR = Path(env.get("TEMPLATE_DIR", "./ingest_templates"))

SFTP_HOST = env.get("SFTP_HOST")
SFTP_USER = env.get("SFTP_USER")
SFTP_PORT = str(env.get("SFTP_PORT", "22"))
SFTP_PRIVATE_KEY_PATH: Path | None = (
    Path(env["SFTP_PRIVATE_KEY_PATH"]) if env.get("SFTP_PRIVATE_KEY_PATH") else None
)
