from typing import List, Optional

from dagster import AssetExecutionContext, AssetKey, Config, MaterializeResult, asset
from dcpy.lifecycle.ingest import get_template_directory, list_ingest_templates
from dcpy.lifecycle.ingest.plan import read_definition_file

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


def make_ingest_asset(template_id: str, depends_on: List[str] | None = None):
    """Create a Dagster asset for an ingest template

    Args:
        template_id: The template identifier
        depends_on: List of template IDs this template depends on
    """
    # Convert template dependencies to Dagster asset keys
    deps = [AssetKey(f"ingest_{dep}") for dep in (depends_on or [])]

    @asset(
        name=f"ingest_{template_id}",
        deps=deps,
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
template_directory = get_template_directory()

# Build dependency map for all templates
template_deps: dict[str, list[str]] = {}
all_template_names = {template.name for template in ingest_templates}

for template in ingest_templates:
    template_path = template_directory / f"{template.name}.yml"

    # Read the template definition using Pydantic models
    # For templates that use {{ version }}, provide a dummy value since:
    # 1. Version is unknown at asset load time (determined at runtime)
    # 2. depends_on field never uses Jinja2 templating
    # 3. We only need depends_on, not the full validated model
    # If the template has syntax errors, this will raise and block deployment
    definition = read_definition_file(template_path, version="dummy_version")
    depends_on = definition.depends_on
    template_deps[template.name] = depends_on

    # Validate dependencies exist
    validate_dependencies(template.name, depends_on, all_template_names)

# Detect circular dependencies for all templates
for template_id in template_deps:
    detect_circular_dependencies(template_deps, template_id)

# Create assets with dependencies
ingest_assets = [
    make_ingest_asset(template.name, template_deps[template.name])
    for template in ingest_templates
]
