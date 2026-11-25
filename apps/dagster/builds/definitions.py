from dagster import Definitions

from .assets import build_assets
from .partition_creator import create_build_partition_asset

# No resources: no build asset requests one. Binding `local_storage` here would also clash with
# ingest's binding when both are merged into the single location in ../definitions.py.
defs = Definitions(
    assets=[*build_assets, create_build_partition_asset],
)
