"""Dagster definitions for build and ingest workflows."""

import os

from builds import build_assets
from ingest import ingest_assets
from ingest.resources import LocalStorageResource

from dagster import Definitions

# Combine all assets
all_assets = [*build_assets, *ingest_assets]

# Use DCPY_LIFECYCLE_DATA_DIR if set (our standard env var for lifecycle operations),
# otherwise fall back to /tmp. This is separate from DAGSTER_HOME which is for
# Dagster's own metadata storage.
storage_base = os.environ.get("DCPY_LIFECYCLE_DATA_DIR", "/tmp/dagster-storage")

# Define resources
resources = {
    "local_storage": LocalStorageResource(base_path=str(storage_base)),
}

# Create Definitions object
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
