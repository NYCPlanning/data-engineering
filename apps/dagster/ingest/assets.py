from typing import List, Optional

from dagster import AssetExecutionContext, Config, MaterializeResult, asset

from dcpy.lifecycle.ingest import get_template_directory, list_ingest_templates

from .partitions import ingest_partition_def
from .resources import LocalStorageResource


class IngestConfig(Config):
    """Configuration for ingest operations.

    Defaults are set for production use (push=True, latest=True).
    For local testing, set push=False and output_csv=True.
    """

    mode: Optional[str] = None  # Preprocessing mode (optional)
    datasets_filter: Optional[List[str]] = None  # Filter specific datasets (optional)
    latest: bool = True  # Push to latest folder in S3
    push: bool = True  # Push results to S3
    output_csv: bool = False  # Also output CSV format (in addition to parquet)
    overwrite_okay: bool = True  # Allow overwriting existing versions


def make_ingest_asset(template_id: str):
    """Create a Dagster asset for an ingest template"""

    @asset(
        name=f"ingest_{template_id}",
        partitions_def=ingest_partition_def,
        group_name="ingest",
        tags={"template": template_id, "lifecycle_stage": "ingest"},
    )
    def _ingest_asset(
        context: AssetExecutionContext,
        config: IngestConfig,
        local_storage: LocalStorageResource,
    ):
        from dcpy.lifecycle.ingest.run import ingest

        partition_key = context.partition_key
        output_path = local_storage.get_path("ingest", template_id, partition_key)

        context.log.info(f"Running ingest for {template_id} version {partition_key}")
        context.log.info(
            f"Config: mode={config.mode}, latest={config.latest}, push={config.push}, "
            f"output_csv={config.output_csv}, overwrite_okay={config.overwrite_okay}"
        )

        ingest(
            dataset_id=template_id,
            mode=config.mode,
            datasets_filter=config.datasets_filter,
            latest=config.latest,
            push=config.push,
            output_csv=config.output_csv,
            definition_dir=get_template_directory(),
            overwrite_okay=config.overwrite_okay,
        )

        return MaterializeResult(
            metadata={
                "output_path": str(output_path),
                "version": partition_key,
                "mode": config.mode,
                "latest": config.latest,
                "push": config.push,
            }
        )

    return _ingest_asset


# Create ingest assets for all templates
ingest_templates = list_ingest_templates()
ingest_assets = [make_ingest_asset(template.name) for template in ingest_templates]
