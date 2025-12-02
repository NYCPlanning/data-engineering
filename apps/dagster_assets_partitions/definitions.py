from assets import ingest_assets, build_asset_groups, distribute_asset_groups
from dagster import Definitions
from resources import local_storage_resource


defs = Definitions(
    assets=ingest_assets
    + sum(build_asset_groups, [])
    + sum(distribute_asset_groups, []),
    resources={
        "local_storage": local_storage_resource,
    },
)
