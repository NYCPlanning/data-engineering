from dagster import Definitions

from .assets import build_assets
from .resources import LocalStorageResource

defs = Definitions(
    assets=build_assets,
    resources={
        "local_storage": LocalStorageResource(base_path="/tmp/dagster-builds"),
    },
)
