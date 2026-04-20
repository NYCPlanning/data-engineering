from pathlib import Path
from typing import List, Optional

from dagster import AssetExecutionContext, Config, MaterializeResult, asset

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


# Mount point for ingest templates in container
INGEST_TEMPLATES_PATH = Path("/app/repos/data-engineering/ingest_templates")


def get_ingest_template_ids() -> List[str]:
    """Get all ingest template IDs from ingest_templates directory"""
    if not INGEST_TEMPLATES_PATH.exists():
        raise FileNotFoundError(
            f"Templates directory not found at {INGEST_TEMPLATES_PATH}. "
            "Ensure the data-engineering repo is mounted correctly at /app/repos/data-engineering"
        )

    templates = []
    for file in INGEST_TEMPLATES_PATH.glob("*.yml"):
        templates.append(file.stem)

    if not templates:
        raise ValueError(
            f"No template files (*.yml) found in {INGEST_TEMPLATES_PATH}. "
            "Check that ingest_templates/ contains .yml files."
        )

    return sorted(templates)


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
            definition_dir=INGEST_TEMPLATES_PATH,
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
ingest_template_ids = get_ingest_template_ids()
ingest_assets = [make_ingest_asset(template_id) for template_id in ingest_template_ids]
