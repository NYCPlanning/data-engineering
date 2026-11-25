"""Show archives already in the datasets store as materialized ingest partitions.

An archive can land without a Dagster run: a bulk copy from another store (DigitalOcean to Azure),
a GitHub Actions ingest, a run on another Dagster instance. ingest_archive_sync reports each
archived version an ingest asset doesn't have yet as a materialization of that partition, adding
the key first for templates with a dynamic partitions def.

It's stopped by default: ingest runs record their own materializations, so it's only needed to
catch up after archives arrive some other way. Switch it on in the UI (Automation) for that, and
off again once a full pass is done.

With the version catalog configured (DCPY_CATALOG_PG_URL), archives come from one catalog query
and a tick checks every template. Without it, each tick takes the next batch of templates,
round-robin, with the cursor holding the index of the next one: one listing of the store per
dataset and one materialized-partitions lookup per template, so a tick stays well inside
Dagster's sensor timeout (60s by default). A full pass over 275 templates at the defaults is 11
ticks. DAGSTER_INGEST_ARCHIVE_SYNC_BATCH_SIZE, when set, applies either way.
"""

import os
from dataclasses import dataclass
from datetime import datetime

from dagster import (
    AddDynamicPartitionsRequest,
    AssetMaterialization,
    DefaultSensorStatus,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dcpy.lifecycle.ingest.version_schedules import is_valid_version

from .assets import TemplateInfo, ingest_asset_key, templates
from .partitions import dynamic_partitions_def_name, ingest_partitions_def

ARCHIVE_SYNC_SOURCE = "archive_sync"
INTERVAL_ENV_VAR = "DAGSTER_INGEST_ARCHIVE_SYNC_INTERVAL_SECONDS"
DEFAULT_INTERVAL_SECONDS = 60
BATCH_SIZE_ENV_VAR = "DAGSTER_INGEST_ARCHIVE_SYNC_BATCH_SIZE"
DEFAULT_BATCH_SIZE = 25


def _interval_seconds() -> int:
    return int(os.environ.get(INTERVAL_ENV_VAR) or DEFAULT_INTERVAL_SECONDS)


def _batch_size(template_count: int, from_catalog: bool) -> int:
    if size := os.environ.get(BATCH_SIZE_ENV_VAR):
        return int(size)
    # The catalog answers for every template in one query; the store takes a listing each.
    return max(template_count, 1) if from_catalog else DEFAULT_BATCH_SIZE


def next_batch(
    template_ids: list[str], cursor: str | None, size: int
) -> tuple[list[str], int]:
    """The templates for this tick, from the cursor's index, and the index to start the next
    tick at, back at 0 after the last batch. A cursor past the end (templates were removed)
    starts over."""
    start = int(cursor) if cursor and cursor.isdigit() else 0
    if start >= len(template_ids):
        start = 0
    end = start + size
    return template_ids[start:end], end if end < len(template_ids) else 0


def _is_partition_key(template: TemplateInfo, version: str) -> bool:
    if schedule := template.version_schedule:
        # One of the schedule's versions, within the years the static keys cover.
        return is_valid_version(schedule, version) and version in set(
            ingest_partitions_def(template.id, schedule).get_partition_keys()
        )
    # Dagster refuses dynamic keys with "..." or control characters.
    return version.isprintable() and "..." not in version


@dataclass(frozen=True)
class ArchivedVersion:
    path: str
    archived_at: datetime | None = None


def catalog_configured() -> bool:
    from dcpy.connectors.edm import catalog

    return catalog.is_configured()


def list_archived_versions_from_catalog(
    templates: list[TemplateInfo],
) -> dict[str, dict[str, ArchivedVersion]]:
    """The same as list_archived_versions, from one query of the version catalog's rows for the
    ingest datasets store."""
    from dcpy.connectors.edm import catalog
    from dcpy.connectors.edm.catalog import ingest_versions
    from dcpy.lifecycle import config

    store = config.connector_name("ingest", "default_datasets_connector")
    dataset_ids = {d for t in templates for d in t.dataset_ids}
    by_dataset: dict[str, dict[str, ArchivedVersion]] = {}
    for row in catalog.query(ingest_versions.DOMAIN, store=store, keys=dataset_ids):
        by_dataset.setdefault(row.key, {})[row.version] = ArchivedVersion(
            (row.location or "").rstrip("/"), row.archived_at
        )
    found: dict[str, dict[str, ArchivedVersion]] = {}
    for template in templates:
        versions = found.setdefault(template.id, {})
        for dataset_id in template.dataset_ids:
            for version, archive in by_dataset.get(dataset_id, {}).items():
                versions.setdefault(version, archive)
    return found


def list_archived_versions(
    templates: list[TemplateInfo],
) -> dict[str, dict[str, ArchivedVersion]]:
    """{template id: {version: archive}} for these templates, from the ingest datasets store.

    The only part of the sensor that reads the store. A data source's version counts if any
    of its datasets has it.
    """
    from dcpy.lifecycle.ingest.connectors import get_processed_datastore_connector

    datastore = get_processed_datastore_connector()
    found: dict[str, dict[str, ArchivedVersion]] = {}
    for template in templates:
        versions = found.setdefault(template.id, {})
        for dataset_id in template.dataset_ids:
            archived = datastore.list_archived_versions(dataset_id).get(dataset_id, {})
            root = str(datastore.storage.storage.root_path).rstrip("/")
            for version, modified in archived.items():
                versions.setdefault(
                    version, ArchivedVersion(f"{root}/{dataset_id}/{version}", modified)
                )
    return found


def sync_archives(
    context: SensorEvaluationContext,
    templates: list[TemplateInfo],
    archived: dict[str, dict[str, ArchivedVersion]],
    cursor: str | None = None,
) -> SensorResult:
    instance = context.instance
    partition_requests: list[AddDynamicPartitionsRequest] = []
    materializations: list[AssetMaterialization] = []
    skipped: list[str] = []

    for template in templates:
        versions = archived.get(template.id, {})
        if not versions:
            continue
        asset_key = ingest_asset_key(template.id)
        materialized = instance.get_materialized_partitions(asset_key)
        keys = [v for v in versions if _is_partition_key(template, v)]
        skipped += [f"{template.id}/{v}" for v in versions if v not in keys]

        if not template.version_schedule:
            partitions_def_name = dynamic_partitions_def_name(template.id)
            existing = set(instance.get_dynamic_partitions(partitions_def_name))
            new_keys = sorted(k for k in keys if k not in existing)
            if new_keys:
                partition_requests.append(
                    AddDynamicPartitionsRequest(
                        partitions_def_name=partitions_def_name, partition_keys=new_keys
                    )
                )

        for version in sorted(k for k in keys if k not in materialized):
            archive = versions[version]
            materializations.append(
                AssetMaterialization(
                    asset_key=asset_key,
                    partition=version,
                    description="Archive found in the datasets store",
                    metadata={
                        "source": ARCHIVE_SYNC_SOURCE,
                        "version": version,
                        "archive_path": archive.path,
                        **(
                            {"archived_at": archive.archived_at.isoformat()}
                            if archive.archived_at
                            else {}
                        ),
                    },
                )
            )

    if skipped:
        context.log.warning(
            f"Skipped {len(skipped)} archived version(s) that aren't valid partition keys "
            f"for their template: {', '.join(sorted(skipped))}"
        )
    context.log.info(
        f"Reporting {len(materializations)} archived version(s) as materialized, adding "
        f"{sum(len(r.partition_keys) for r in partition_requests)} partition key(s)"
    )
    return SensorResult(
        cursor=cursor,
        asset_events=materializations,
        dynamic_partitions_requests=partition_requests,
    )


@sensor(
    name="ingest_archive_sync",
    minimum_interval_seconds=_interval_seconds(),
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Reports versions already in the ingest datasets store as materialized partitions "
        "of their ingest asset, for archives Dagster didn't write."
    ),
)
def ingest_archive_sync(context: SensorEvaluationContext):
    # Evaluated in the code server (the runner), which has the store's credentials. Anything
    # wrong with them fails this tick, not the code location.
    template_ids = sorted(templates)
    archived = None
    if catalog_configured():
        batch, next_index = next_batch(
            template_ids, context.cursor, _batch_size(len(template_ids), True)
        )
        batch_templates = [templates[t] for t in batch]
        try:
            archived = list_archived_versions_from_catalog(batch_templates)
            context.log.info(
                f"Read archives for {len(batch)} template(s) from the catalog"
            )
        except Exception as e:
            # Fall back to the store, a normal-sized batch at a time.
            context.log.warning(f"Catalog unavailable, listing the store instead: {e}")
    if archived is None:
        batch, next_index = next_batch(
            template_ids, context.cursor, _batch_size(len(template_ids), False)
        )
        batch_templates = [templates[t] for t in batch]
        archived = list_archived_versions(batch_templates)
        context.log.info(f"Listed the store for {len(batch)} template(s)")
    context.log.info(f"Checking {len(batch)} template(s), {batch[0]} to {batch[-1]}")
    return sync_archives(context, batch_templates, archived, cursor=str(next_index))
