import os

from dagster import Definitions

from .assets import ingest_assets
from .resources import LocalStorageResource
from .sensors import ingest_archive_sync

# Use DCPY_LIFECYCLE_DATA_DIR if set, otherwise fall back to /tmp
storage_base = os.environ.get("DCPY_LIFECYCLE_DATA_DIR", "/tmp/dagster-storage")

# No job across all ingest assets: they don't share a partitions def (see partitions.py), and a
# job's assets must. Materialize them from the asset pages, one template or family at a time.
defs = Definitions(
    assets=ingest_assets,
    sensors=[ingest_archive_sync],
    resources={
        "local_storage": LocalStorageResource(base_path=storage_base),
    },
)
