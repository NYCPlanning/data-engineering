import os

from dagster import Definitions

from .assets import ingest_assets
from .jobs import ingest_all_job, ingest_dev_job
from .resources import LocalStorageResource

# Use DCPY_LIFECYCLE_DATA_DIR if set, otherwise fall back to /tmp
storage_base = os.environ.get("DCPY_LIFECYCLE_DATA_DIR", "/tmp/dagster-storage")

defs = Definitions(
    assets=ingest_assets,
    jobs=[ingest_all_job, ingest_dev_job],
    resources={
        "local_storage": LocalStorageResource(base_path=storage_base),
    },
)
