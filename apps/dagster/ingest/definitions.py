import os

from dagster import Definitions

from .assets import ingest_assets
from .jobs import ingest_all_job, ingest_dev_job
from .ops import cleanup_data_job
from .resources import LocalStorageResource
from .schedules import cleanup_schedule

# Use DCPY_LIFECYCLE_DATA_DIR if set, otherwise fall back to /tmp
storage_base = os.environ.get("DCPY_LIFECYCLE_DATA_DIR", "/tmp/dagster-storage")

defs = Definitions(
    assets=ingest_assets,
    jobs=[ingest_all_job, ingest_dev_job, cleanup_data_job],
    schedules=[cleanup_schedule],
    resources={
        "local_storage": LocalStorageResource(base_path=storage_base),
    },
)
