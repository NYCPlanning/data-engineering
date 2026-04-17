from dagster import Definitions

from .assets import ingest_assets
from .jobs import ingest_all_job, ingest_dev_job
from .resources import LocalStorageResource

defs = Definitions(
    assets=ingest_assets,
    jobs=[ingest_all_job, ingest_dev_job],
    resources={
        "local_storage": LocalStorageResource(base_path="/opt/dagster/storage"),
    },
)
