"""version_catalog: refreshes the `ingest` rows of the version catalog from the datasets store(s).

The catalog (dcpy.services.catalog, in the database DCPY_CATALOG_PG_URL points at) is
what build planning and ingest_archive_sync read instead of listing the store. Ingests write
their archives through to it (dcpy-services' handler for lifecycle's DatasetsArchived event);
this sweep catches everything else (bulk copies, ingests without the catalog configured,
deletions) and runs daily. Unpartitioned, and independent of the ingest assets.
"""

import time

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    Failure,
    MaterializeResult,
    ScheduleDefinition,
    asset,
)

AZURE_DATASETS_CONNECTOR = "az.edm.recipes.datasets"


def catalog_stores() -> list[str]:
    """The connector ingest writes to, plus the Azure family's datasets store if it's registered
    and isn't that one."""
    from dcpy.lifecycle import config
    from dcpy.lifecycle.connector_registry import connectors

    stores = [config.connector_name("ingest", "default_datasets_connector")]
    if (
        AZURE_DATASETS_CONNECTOR in connectors.list_registered()
        and AZURE_DATASETS_CONNECTOR not in stores
    ):
        stores.append(AZURE_DATASETS_CONNECTOR)
    return stores


@asset(
    name="version_catalog",
    group_name="catalog",
    description=(
        "Which versions of each ingest dataset exist in the datasets store(s), in the "
        "version catalog's catalog.versions table."
    ),
)
def version_catalog(context: AssetExecutionContext) -> MaterializeResult:
    from dcpy.connectors.ingest_datastore import Connector as IngestDatastore
    from dcpy.lifecycle.connector_registry import connectors
    from dcpy.services import catalog
    from dcpy.services.catalog import ingest_versions

    if not catalog.is_configured():
        raise Failure(f"{catalog.CATALOG_PG_URL_ENV_VAR} isn't set on the runner")

    start = time.monotonic()
    applied = catalog.migrate()
    if applied:
        context.log.info(f"Applied catalog migrations: {', '.join(applied)}")

    metadata: dict = {"migrations_applied": applied}
    failed: list[str] = []
    for store in catalog_stores():
        try:
            result = ingest_versions.refresh_store(
                connectors[store, IngestDatastore], store
            )
        except Exception as e:
            context.log.error(f"Refreshing {store} failed: {e}")
            failed.append(store)
            continue
        for error in result.stats.errors[:20]:
            context.log.warning(f"{store}: {error}")
        metadata[store] = {
            "versions": result.stats.versions,
            "added": result.counts.added,
            "updated": result.counts.updated,
            "removed": result.counts.removed,
            "configs_read": result.stats.configs_read,
            "errors": len(result.stats.errors),
            "seconds": round(result.seconds, 1),
        }
        context.log.info(f"{store}: {metadata[store]}")

    metadata["duration_seconds"] = round(time.monotonic() - start, 1)
    if failed:
        raise Failure(
            f"Couldn't refresh {', '.join(failed)}; see the log",
            metadata=metadata,
        )
    return MaterializeResult(metadata=metadata)


version_catalog_daily = ScheduleDefinition(
    name="version_catalog_daily",
    target=version_catalog,
    cron_schedule="0 6 * * *",
    execution_timezone="America/New_York",
    default_status=DefaultScheduleStatus.RUNNING,
)
