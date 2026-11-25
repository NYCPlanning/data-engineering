"""Version catalog: which versions exist for an asset, per store, in Postgres.

It's a cache of what's in the stores, never the source of truth: readers fall back to the store
when a row is missing or the catalog can't be reached. Set DCPY_CATALOG_PG_URL (a postgres URL)
to turn it on; unset, nothing reads or writes it.

- `db`: the `catalog.versions` table (connect, migrate, upsert, query).
- `ingest_versions`: `ingest` rows, built from an ingest datastore's listing (refresh_store).
- `lifecycle_events.handle`: writes through what ingest archives. Registered as a
  `dcpy.lifecycle.event_handlers` entry point, so any process with dcpy-services installed and
  the catalog configured records ingests, whoever runs them.
- `version_index.CatalogVersionIndex`: answers build planning's inputs
  (dcpy.lifecycle.builds.version_index.VersionIndex).

    python -m dcpy.services catalog migrate
    python -m dcpy.services catalog show --domain ingest --key dcp_ct2010
"""

from . import ingest_versions, lifecycle_events
from .db import (
    CATALOG_PG_URL_ENV_VAR,
    CatalogNotConfigured,
    VersionRow,
    WriteCounts,
    connect,
    is_configured,
    migrate,
    migration_files,
    pg_url,
    query,
    set_latest,
    sync_store,
    upsert,
)

__all__ = [
    "CATALOG_PG_URL_ENV_VAR",
    "CatalogNotConfigured",
    "VersionRow",
    "WriteCounts",
    "connect",
    "ingest_versions",
    "lifecycle_events",
    "is_configured",
    "migrate",
    "migration_files",
    "pg_url",
    "query",
    "set_latest",
    "sync_store",
    "upsert",
]
