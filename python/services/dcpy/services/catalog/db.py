"""The catalog's Postgres table: connect, migrate, write and query rows.

One table, `catalog.versions`, keyed by (domain, store, key, version). `domain` says what kind of
asset a row is (`ingest` for processed ingest datasets; builds, drafts and published later),
`store` is the registry name of the connector it lives behind, so rows for two stores holding the
same datasets (DigitalOcean and Azure) sit side by side.

Migrations are the numbered SQL files in ./migrations, applied in order by `migrate`.
"""

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator

import psycopg2
from psycopg2.extras import Json, execute_values

from dcpy.utils.logging import logger

CATALOG_PG_URL_ENV_VAR = "DCPY_CATALOG_PG_URL"
SCHEMA = "catalog"
MIGRATIONS_DIR = Path(__file__).parent / "migrations"
# pg_advisory_lock key held while migrating, so concurrent migrate() calls apply each file once.
_MIGRATION_LOCK_ID = 4_172_635_001
# Readers fall back to the store, so a catalog that doesn't answer shouldn't hold them up.
CONNECT_TIMEOUT_SECONDS = 10


class CatalogNotConfigured(RuntimeError):
    pass


def pg_url() -> str | None:
    """DCPY_CATALOG_PG_URL, read at call time. None (catalog off) when unset or empty."""
    return (os.environ.get(CATALOG_PG_URL_ENV_VAR) or "").strip() or None


def is_configured() -> bool:
    return pg_url() is not None


@contextmanager
def connect(conn_url: str | None = None) -> Iterator["psycopg2.extensions.connection"]:
    url = conn_url or pg_url()
    if not url:
        raise CatalogNotConfigured(f"{CATALOG_PG_URL_ENV_VAR} is not set")
    conn = psycopg2.connect(url, connect_timeout=CONNECT_TIMEOUT_SECONDS)
    try:
        yield conn
    finally:
        conn.close()


@dataclass
class VersionRow:
    domain: str
    store: str
    key: str
    version: str
    location: str | None = None
    file_types: list[str] = field(default_factory=list)
    archived_at: datetime | None = None
    is_latest: bool = False
    details: dict = field(default_factory=dict)
    # Set by the database; ignored on write.
    first_seen_at: datetime | None = None
    refreshed_at: datetime | None = None


_COLUMNS = [
    "domain",
    "store",
    "key",
    "version",
    "location",
    "file_types",
    "archived_at",
    "is_latest",
    "details",
    "first_seen_at",
    "refreshed_at",
]
_WRITE_COLUMNS = _COLUMNS[:9]


def _values(row: VersionRow) -> tuple:
    return (
        row.domain,
        row.store,
        row.key,
        row.version,
        row.location,
        list(row.file_types),
        row.archived_at,
        row.is_latest,
        Json(row.details),
    )


## Migrations


def migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9][0-9]_*.sql"))


def migrate(conn_url: str | None = None) -> list[str]:
    """Apply pending migrations in order, each in its own transaction. Returns the files applied.

    Idempotent, and safe to run from several processes at once: they queue on an advisory lock.
    Everything, schema_migrations included, lives in the `catalog` schema, never `public`. Needs
    no superuser: creates the schema if it's missing (the database owner can), or uses one made
    for the role (CREATE SCHEMA catalog AUTHORIZATION <role>).
    """
    applied_now: list[str] = []
    with connect(conn_url) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (_MIGRATION_LOCK_ID,))
            conn.commit()
            try:
                # Only when it's missing: CREATE SCHEMA IF NOT EXISTS still needs CREATE on
                # the database, which a role given a ready-made schema may not have.
                cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (SCHEMA,))
                if cur.fetchone() is None:
                    cur.execute(f"CREATE SCHEMA {SCHEMA}")
                cur.execute(
                    f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.schema_migrations (
                        version text PRIMARY KEY,
                        name text NOT NULL,
                        applied_at timestamptz NOT NULL DEFAULT now()
                    )"""
                )
                conn.commit()
                cur.execute(f"SELECT version FROM {SCHEMA}.schema_migrations")
                done = {r[0] for r in cur.fetchall()}
                conn.commit()
                for path in migration_files():
                    version = path.name.split("_", 1)[0]
                    if version in done:
                        continue
                    logger.info(f"Catalog: applying migration {path.name}")
                    try:
                        cur.execute(path.read_text())
                        cur.execute(
                            f"INSERT INTO {SCHEMA}.schema_migrations (version, name) "
                            "VALUES (%s, %s)",
                            (version, path.name),
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    applied_now.append(path.name)
            finally:
                conn.rollback()
                cur.execute("SELECT pg_advisory_unlock(%s)", (_MIGRATION_LOCK_ID,))
                conn.commit()
    return applied_now


## Writes


@dataclass
class WriteCounts:
    added: int = 0
    updated: int = 0
    removed: int = 0
    unchanged: int = 0


def _upsert(cur, rows: list[VersionRow], update_is_latest: bool = True) -> WriteCounts:
    """Insert or update rows; a row whose content is the same is left alone (counted as
    unchanged). Without `update_is_latest`, an existing row keeps its is_latest."""
    counts = WriteCounts()
    if not rows:
        return counts
    cols = ", ".join(_WRITE_COLUMNS)
    updatable = ["location", "file_types", "archived_at", "details"]
    if update_is_latest:
        updatable.append("is_latest")
    results = execute_values(
        cur,
        f"""INSERT INTO {SCHEMA}.versions AS v ({cols}) VALUES %s
        ON CONFLICT (domain, store, key, version) DO UPDATE SET
            {", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)},
            refreshed_at = now()
        WHERE ({", ".join(f"v.{c}" for c in updatable)})
            IS DISTINCT FROM ({", ".join(f"EXCLUDED.{c}" for c in updatable)})
        RETURNING (xmax = 0)""",
        [_values(r) for r in rows],
        page_size=1000,
        fetch=True,
    )
    counts.added = sum(1 for (inserted,) in results if inserted)
    counts.updated = len(results) - counts.added
    counts.unchanged = len(rows) - len(results)
    return counts


def upsert(
    rows: Iterable[VersionRow],
    *,
    update_is_latest: bool = True,
    conn_url: str | None = None,
) -> WriteCounts:
    rows = list(rows)
    with connect(conn_url) as conn:
        with conn, conn.cursor() as cur:
            return _upsert(cur, rows, update_is_latest=update_is_latest)


def set_latest(
    domain: str, store: str, key: str, version: str, conn_url: str | None = None
) -> bool:
    """Mark `version` as the key's latest, and no other. False if there's no row for it (then
    nothing changes: the next refresh sorts it out)."""
    with connect(conn_url) as conn:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"""UPDATE {SCHEMA}.versions SET is_latest = (version = %(version)s),
                    refreshed_at = now()
                WHERE domain = %(domain)s AND store = %(store)s AND key = %(key)s
                    AND EXISTS (
                        SELECT 1 FROM {SCHEMA}.versions
                        WHERE domain = %(domain)s AND store = %(store)s AND key = %(key)s
                            AND version = %(version)s
                    )
                    AND is_latest IS DISTINCT FROM (version = %(version)s)""",
                {"domain": domain, "store": store, "key": key, "version": version},
            )
            cur.execute(
                f"SELECT 1 FROM {SCHEMA}.versions WHERE domain = %s AND store = %s "
                "AND key = %s AND version = %s",
                (domain, store, key, version),
            )
            return cur.fetchone() is not None


def sync_store(
    domain: str,
    store: str,
    rows: Iterable[VersionRow],
    *,
    allow_empty: bool = False,
    conn_url: str | None = None,
) -> WriteCounts:
    """Make the catalog's rows for (domain, store) exactly `rows`, in one transaction: upsert
    them, delete the rest.

    An empty `rows` against a store that has rows is refused unless `allow_empty`: it's more
    likely a misconfigured listing than a store that was emptied.
    """
    rows = list(rows)
    assert all(r.domain == domain and r.store == store for r in rows), (
        f"rows must all be for domain {domain}, store {store}"
    )
    with connect(conn_url) as conn:
        with conn, conn.cursor() as cur:
            if not rows and not allow_empty:
                cur.execute(
                    f"SELECT count(*) FROM {SCHEMA}.versions WHERE domain = %s AND store = %s",
                    (domain, store),
                )
                (existing,) = cur.fetchone()  # type: ignore[misc]
                if existing:
                    raise ValueError(
                        f"Listing of {store} found nothing, but the catalog has {existing} "
                        f"{domain} row(s) for it. Not deleting them; pass allow_empty to."
                    )
            counts = _upsert(cur, rows)
            cur.execute(
                "CREATE TEMP TABLE _catalog_present (key text, version text) ON COMMIT DROP"
            )
            execute_values(
                cur,
                "INSERT INTO _catalog_present (key, version) VALUES %s",
                [(r.key, r.version) for r in rows],
                page_size=5000,
            )
            cur.execute(
                f"""DELETE FROM {SCHEMA}.versions v
                WHERE v.domain = %s AND v.store = %s AND NOT EXISTS (
                    SELECT 1 FROM _catalog_present p
                    WHERE p.key = v.key AND p.version = v.version
                )""",
                (domain, store),
            )
            counts.removed = cur.rowcount
            cur.execute(
                f"UPDATE {SCHEMA}.versions SET refreshed_at = now() "
                "WHERE domain = %s AND store = %s",
                (domain, store),
            )
            return counts


## Reads


def query(
    domain: str,
    *,
    store: str | None = None,
    keys: Iterable[str] | None = None,
    latest_only: bool = False,
    conn_url: str | None = None,
) -> list[VersionRow]:
    """Rows for `domain`, optionally narrowed to one store, some keys, or latest versions."""
    where = ["domain = %s"]
    params: list = [domain]
    if store is not None:
        where.append("store = %s")
        params.append(store)
    if keys is not None:
        where.append("key = ANY(%s)")
        params.append(list(keys))
    if latest_only:
        where.append("is_latest")
    with connect(conn_url) as conn:
        with conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM {SCHEMA}.versions "
                f"WHERE {' AND '.join(where)} ORDER BY store, key, version",
                params,
            )
            return [
                VersionRow(**dict(zip(_COLUMNS, r, strict=True)))
                for r in cur.fetchall()
            ]
