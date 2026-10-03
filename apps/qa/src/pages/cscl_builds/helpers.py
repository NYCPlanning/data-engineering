import os
import re
from datetime import datetime
from urllib.parse import quote

from dcpy.lifecycle.builds.artifacts import drafts
from dcpy.lifecycle.ingest.connectors import get_processed_datastore_connector

PRODUCT = "db-cscl"
RAW_DATASET_ID = "dcp_cscl_gdb"
# The raw fgdb archive (edm.recipes.raw_datasets, id dcp_cscl_gdb) is keyed by extraction
# timestamp, not the version typed in at ingest time - the clean version string only shows up
# on the ~50 per-layer datasets it unpacks into (edm.recipes.datasets). Any one of them works
# as a stand-in for "is this version ingested", since they're all archived together in the
# same ingest run.
PROCESSED_DATASET_ID = "dcp_cscl_centerline"
RECIPES_BUCKET = "edm-recipes"
INBOX_BUCKET = "edm-private"


def get_ingested_versions() -> list[str]:
    """Versions of the CSCL fgdb that have been fully ingested, newest first.

    Re-sorted case-insensitively on top of the connector's own (case-sensitive) sort: version
    casing isn't enforced at ingest time, so a stray different-cased version shouldn't sort as
    if it were a wildly different value than its same-letters counterpart.
    """
    versions = get_processed_datastore_connector().list_versions(
        PROCESSED_DATASET_ID, sort_desc=True
    )
    return sorted((v for v in versions if v != "latest"), key=str.lower, reverse=True)


def get_draft_revisions(ingested_version: str) -> list[str]:
    """Draft revisions (e.g. '2-my-note') already promoted for this ingested version."""
    return drafts.get_dataset_version_revisions(PRODUCT, ingested_version)


def directory_url(bucket: str, path: str) -> str:
    """Unsigned DO Spaces folder-browser link. Build/draft output is already public-read."""
    endpoint = os.environ["AWS_S3_ENDPOINT"].rstrip("/")
    return f"{endpoint}/{bucket}/{quote(path)}"


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip()).strip("-").lower()
    return slug[:40]


def build_name_for(ingested_version: str, build_note: str) -> str:
    """A build_name is used downstream (bash/build_env_setup.sh) as an unquoted Postgres
    schema name - valid identifiers can't start with a digit, which CSCL's own version
    strings ("26c", "26a", ...) always do. The "cscl" prefix isn't just descriptive, it's
    what keeps the derived schema name legal regardless of the version string's shape.
    """
    timestamp = datetime.now().strftime("%Y%m%dT%H%M")
    slug = slugify(build_note)
    parts = ["cscl", ingested_version, *([slug] if slug else []), timestamp]
    return "_".join(parts)
