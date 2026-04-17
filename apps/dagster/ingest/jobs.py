"""Jobs for running ingest operations with preset configurations"""

from dagster import AssetSelection, define_asset_job

# Job that runs all ingest assets (use default config in launchpad)
ingest_all_job = define_asset_job(
    name="ingest_all",
    selection=AssetSelection.groups("ingest"),
    description="Run all ingest assets with configurable settings",
)

# Job for dev/testing - doesn't push to S3
ingest_dev_job = define_asset_job(
    name="ingest_dev",
    selection=AssetSelection.groups("ingest"),
    description="Run ingest assets in dev mode (no S3 push, outputs CSV)",
    config={
        "ops": {
            "*": {
                "config": {
                    "latest": False,
                    "push": False,
                    "output_csv": True,
                }
            }
        }
    },
)
