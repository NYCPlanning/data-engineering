from dagster import Definitions

from .assets import ingest_assets
from .jobs import ingest_all_job, ingest_dev_job
from .ops import cleanup_data_job
from .resources import LocalStorageResource
from .schedules import cleanup_schedule

defs = Definitions(
    assets=ingest_assets,
    jobs=[ingest_all_job, ingest_dev_job, cleanup_data_job],
    schedules=[cleanup_schedule],
    resources={
        "local_storage": LocalStorageResource(base_path="/opt/dagster/storage"),
    },
)
