from os import environ as env
from pathlib import Path

from dcpy.utils.logging import logger

CI = "CI" in env

BUILD_NAME = env.get("BUILD_NAME")


# Set BUILD_ENGINE_SCHEMA in environment if not already set
# Used by recipe templates (via BUILD_ENGINE_* variable filtering)
if "BUILD_ENGINE_SCHEMA" not in env:
    try:
        from dcpy.utils import git

        branch = git.branch()
        # DB schema names can't use dashes
        sanitized_branch = branch.lower().replace("-", "_")
        env["BUILD_ENGINE_SCHEMA"] = sanitized_branch
        logger.info(
            f"Set BUILD_ENGINE_SCHEMA to sanitized branch name: {sanitized_branch}"
        )
    except Exception as e:
        # If we can't get git branch (e.g., not in a git repo), leave it unset
        logger.debug(f"Could not set BUILD_ENGINE_SCHEMA from git branch: {e}")

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

# Project root directory (assumes dcpy is at PROJECT_ROOT/dcpy)
PROJECT_ROOT_PATH = Path(__file__).parent.parent


# Migrated product metadata from its own repo to a top-level directory in this repo
# (product-metadata/, next to products/). Resolve it relative to the runtime checkout
# rather than dcpy's install location: when dcpy is installed into an image
# (site-packages), __file__ can't see the checked-out repo. Walking up from the working
# directory finds the checkout we're actually running in, so dev-branch metadata changes
# are picked up without rebuilding images or setting the path in every workflow — the
# same way the products/ directory is already referenced by path.
def _default_product_metadata_repo_path() -> Path:
    # An explicit override wins (containers that set it, or pointing at another checkout).
    override = env.get("PRODUCT_METADATA_REPO_PATH")
    if override:
        return Path(override)
    # Otherwise find the checkout by walking up from the working directory.
    for directory in (Path.cwd(), *Path.cwd().parents):
        candidate = directory / "product-metadata"
        if (candidate / "metadata.yml").exists():
            return candidate
    # Fall back to the package-relative copy (editable / checkout installs).
    return PROJECT_ROOT_PATH / "product-metadata"


PRODUCT_METADATA_REPO_PATH = _default_product_metadata_repo_path()
if not PRODUCT_METADATA_REPO_PATH.exists():
    logger.warning(
        f"PRODUCT_METADATA_REPO_PATH: {PRODUCT_METADATA_REPO_PATH} points to a nonexistent path"
    )

INGEST_DEF_DIR = Path(env.get("TEMPLATE_DIR", "./ingest_templates"))
if not INGEST_DEF_DIR:
    logger.warning("warning: INGEST_DEF_DIR is not set. Ingest will not function.")
elif not Path(INGEST_DEF_DIR).exists():
    logger.warning(f"INGEST_DEF_DIR: {INGEST_DEF_DIR} points to a nonexistent path")

# Products directory for build recipes
PRODUCTS_DIR = Path(env.get("PRODUCTS_DIR", PROJECT_ROOT_PATH / "products"))

SFTP_HOST = env.get("SFTP_HOST")
SFTP_USER = env.get("SFTP_USER")
SFTP_PORT = str(env.get("SFTP_PORT", "22"))
SFTP_PRIVATE_KEY_PATH: Path | None = (
    Path(env["SFTP_PRIVATE_KEY_PATH"]) if env.get("SFTP_PRIVATE_KEY_PATH") else None
)

# Build engine configuration (set by recipe env vars)
BUILD_ENGINE_DB = env.get("BUILD_ENGINE_DB")
BUILD_ENGINE_SCHEMA = env.get("BUILD_ENGINE_SCHEMA")
BUILD_ENV_OUTPUT_DIR = env.get("BUILD_ENV_OUTPUT_DIR")
