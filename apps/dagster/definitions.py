"""Dagster definitions for build and ingest workflows."""

from builds import LocalStorageResource as BuildLocalStorageResource
from builds import build_assets
from dagster import Definitions
from ingest import ingest_assets

# Combine all assets
all_assets = [*build_assets, *ingest_assets]

# Define resources
resources = {
    "local_storage": BuildLocalStorageResource(base_path="/tmp/dagster-builds"),
}

# Create Definitions object
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
