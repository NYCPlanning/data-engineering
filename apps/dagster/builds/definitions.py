from dagster import Definitions

from .assets import build_assets
from .partition_creator import create_build_partition_asset
from .resources import LocalStorageResource

defs = Definitions(
    assets=[*build_assets, create_build_partition_asset],
    resources={
        "local_storage": LocalStorageResource(base_path="/tmp/dagster-builds"),
    },
)
