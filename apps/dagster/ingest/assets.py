import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dagster import AssetExecutionContext, AssetKey, Config, MaterializeResult, asset
from dcpy.lifecycle.ingest import get_template_directory, list_ingest_templates
from dcpy.lifecycle.ingest.models import DatasetDefinition
from dcpy.lifecycle.ingest.plan import get_jinja_vars, read_definition_file
from dcpy.lifecycle.ingest.version_schedules import VersionSchedule

from .partitions import ingest_partitions_def
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


def validate_dependencies(
    template_id: str, depends_on: List[str], all_template_names: set[str]
) -> None:
    """Validate that all dependencies exist and there are no circular dependencies.

    Args:
        template_id: The ID of the template being validated
        depends_on: List of template IDs this template depends on
        all_template_names: Set of all valid template names

    Raises:
        ValueError: If dependencies don't exist or circular dependencies detected
    """
    # Check that all dependencies exist
    missing_deps = [dep for dep in depends_on if dep not in all_template_names]
    if missing_deps:
        raise ValueError(
            f"Template '{template_id}' has dependencies on non-existent templates: {missing_deps}"
        )


def detect_circular_dependencies(
    template_deps: dict[str, list[str]],
    template_id: str,
    visited: set[str] | None = None,
) -> None:
    """Detect circular dependencies in template dependency graph.

    Args:
        template_deps: Dictionary mapping template IDs to their dependencies
        template_id: The current template being checked
        visited: Set of templates already visited in this path

    Raises:
        ValueError: If a circular dependency is detected
    """
    if visited is None:
        visited = set()

    if template_id in visited:
        cycle_path = " -> ".join(list(visited) + [template_id])
        raise ValueError(f"Circular dependency detected: {cycle_path}")

    visited.add(template_id)

    for dep in template_deps.get(template_id, []):
        detect_circular_dependencies(template_deps, dep, visited.copy())


def template_takes_version(template_path: Path) -> bool:
    """Whether the template has a {{ version }} placeholder that the run has to fill in."""
    return "version" in get_jinja_vars(template_path.read_text())


def _keep_staging() -> bool:
    return os.environ.get("DCPY_KEEP_STAGING", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _remove_staging_dir(path: Path, stop_at: Path) -> None:
    """Delete a run's staging dir, then its parents (partition, template) while they're empty,
    stopping at the storage base."""
    shutil.rmtree(path, ignore_errors=True)
    for parent in path.parents:
        if parent == stop_at or stop_at not in parent.parents:
            break
        try:
            parent.rmdir()
        except OSError:
            break  # not empty: another run's staging is in it


def ingest_asset_key(template_id: str) -> AssetKey:
    return AssetKey(f"ingest_{template_id}")


def make_ingest_asset(
    template_id: str,
    depends_on: List[str] | None = None,
    version_schedule: VersionSchedule | None = None,
):
    """Create a Dagster asset for an ingest template

    Args:
        template_id: The template identifier
        depends_on: List of template IDs this template depends on
        version_schedule: The template's shared version schedule, if it has one
    """
    # Convert template dependencies to Dagster asset keys
    deps = [ingest_asset_key(dep) for dep in (depends_on or [])]

    @asset(
        name=f"ingest_{template_id}",
        deps=deps,
        partitions_def=ingest_partitions_def(template_id, version_schedule),
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
        # One staging dir per run: ingest's default is a single shared directory that each run
        # wipes on start, so two runs on the runner at once would clobber each other.
        output_path = local_storage.get_path(
            "ingest", template_id, partition_key, context.run_id
        )

        context.log.info(f"Running ingest for {template_id} version {partition_key}")
        context.log.info(
            f"Config: mode={config.mode}, latest={config.latest}, push={config.push}, "
            f"output_csv={config.output_csv}, overwrite_okay={config.overwrite_okay}"
        )

        # The partition key is the version for templates that are parameterized by one (e.g. an
        # inbox file path). Others resolve their version from the source.
        template_dir = get_template_directory()
        version = (
            partition_key
            if template_takes_version(template_dir / f"{template_id}.yml")
            else None
        )

        # The archive in object storage is the result; the staging dir is scratch, so it goes
        # when the run ends, pass or fail. Kept for a dry run (push: false, where it holds the
        # only output) or when DCPY_KEEP_STAGING is set, for debugging.
        keep_staging = not config.push or _keep_staging()
        try:
            datasets = ingest(
                dataset_id=template_id,
                version=version,
                staging_dir=output_path,
                mode=config.mode,
                datasets_filter=config.datasets_filter,
                latest=config.latest,
                push=config.push,
                output_csv=config.output_csv,
                definition_dir=template_dir,
                overwrite_okay=config.overwrite_okay,
            )
        finally:
            if keep_staging:
                context.log.info(f"Keeping staging dir {output_path}")
            else:
                _remove_staging_dir(output_path, stop_at=local_storage.get_path())
                context.log.info(f"Removed staging dir {output_path}")

        return MaterializeResult(
            metadata={
                # Only when the files are still there to look at.
                **({"output_path": str(output_path)} if keep_staging else {}),
                "version": partition_key,
                # What was archived: without a {{ version }} placeholder, ingest resolves the
                # version from the source, so it can differ from the partition key.
                "archived_versions": sorted({ds.version for ds in datasets}),
                "mode": config.mode,
                "latest": config.latest,
                "push": config.push,
            }
        )

    return _ingest_asset


@dataclass(frozen=True)
class TemplateInfo:
    id: str
    depends_on: list[str]
    version_schedule: VersionSchedule | None
    # Ids the template archives under: its own, or each downstream dataset's for a data source
    dataset_ids: tuple[str, ...]


def read_template_info(template_path: Path) -> TemplateInfo:
    # Version is unknown at load time (it's picked per run), so render {{ version }} with a
    # dummy: only depends_on, version_schedule and the dataset ids are needed, and none of
    # them are templated. A template with syntax errors raises here and blocks the deploy.
    definition = read_definition_file(template_path, version="dummy_version")
    template_id = template_path.stem
    if definition.version_schedule and not template_takes_version(template_path):
        raise ValueError(
            f"Template '{template_id}' has version_schedule "
            f"'{definition.version_schedule}' but no {{{{ version }}}} placeholder"
        )
    dataset_ids = (
        (definition.id,)
        if isinstance(definition, DatasetDefinition)
        else tuple(d.id for d in definition.datasets)
    )
    return TemplateInfo(
        id=template_id,
        depends_on=definition.depends_on,
        version_schedule=definition.version_schedule,
        dataset_ids=dataset_ids,
    )


# Create ingest assets for all templates
ingest_templates = list_ingest_templates()
template_directory = get_template_directory()

templates: dict[str, TemplateInfo] = {
    template.name: read_template_info(template_directory / f"{template.name}.yml")
    for template in ingest_templates
}
template_deps = {t.id: t.depends_on for t in templates.values()}
all_template_names = set(templates)

for template_id, depends_on in template_deps.items():
    validate_dependencies(template_id, depends_on, all_template_names)

# Detect circular dependencies for all templates
for template_id in template_deps:
    detect_circular_dependencies(template_deps, template_id)

# Create assets with dependencies
ingest_assets = [
    make_ingest_asset(t.id, t.depends_on, t.version_schedule)
    for t in templates.values()
]
