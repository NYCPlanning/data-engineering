"""Catalog rows for the `ingest` domain: processed datasets in an ingest datastore.

A row is a `<key>/<version>/` folder with a config.json (what ingest writes with the data, and
what ingest_datastore.Connector.list_archived_versions counts). `latest/` and library archives
set aside as `<version>_library/` aren't versions; `latest/config.json` says which version is
latest.

A refresh lists the store once, then reads config.json only for versions that are new or whose
config.json changed since the last refresh, and latest/config.json only where it changed.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from dcpy.connectors.edm.models import (
    DatasetType,
    SparseConfig,
    archive_timestamp_from_config,
)
from dcpy.connectors.ingest_datastore import Connector as IngestDatastore
from dcpy.connectors.ingest_datastore import config_filename
from dcpy.utils import s3_metadata
from dcpy.utils.logging import logger

from . import VersionRow, WriteCounts, query, set_latest, sync_store, upsert

DOMAIN = "ingest"
LATEST = "latest"
# Concurrent config.json reads during a refresh.
MAX_WORKERS = 16

# details keys
NAME = "name"
URL = "url"
# archived_at as the config wrote it, with its own UTC offset: timestamptz keeps the instant but
# not the offset, and a build's archive_date is the date in the original offset.
ARCHIVED_AT_ISO = "archived_at_iso"
# config.json's LastModified when it was last read, to skip rereading an unchanged one
CONFIG_MODIFIED = "config_modified"
# on the latest row: latest/config.json's LastModified when it was last read
LATEST_CONFIG_MODIFIED = "latest_config_modified"
CONFIG_ERROR = "config_error"

Files = dict[
    str, datetime | None
]  # file path under the version folder -> last modified


def group_listing(
    listing: Iterable[tuple[str, datetime | None]],
) -> dict[str, dict[str, Files]]:
    """{key: {folder: {file: last modified}}} from (path, last modified) pairs relative to the
    store's root, for `<key>/<folder>/<file...>`. Folders include `latest` and `_library` ones."""
    grouped: dict[str, dict[str, Files]] = {}
    for path, modified in listing:
        parts = path.split("/", 2)
        if len(parts) != 3 or not parts[2]:
            continue
        key, folder, file = parts
        grouped.setdefault(key, {}).setdefault(folder, {})[file] = modified
    return grouped


def is_version_folder(folder: str, files: Files) -> bool:
    return (
        folder != LATEST
        and not folder.endswith("_library")
        and config_filename in files
    )


def file_types(files: Iterable[str]) -> list[str]:
    """Dataset types of the files, by extension, as dcpy.connectors.edm.recipes.get_file_types
    sees them."""
    types = {DatasetType.from_extension(Path(f).suffix.strip(".")) for f in files}
    return sorted(t.value for t in types if t is not None)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def details_from_config(config: dict) -> dict:
    attributes = config.get("attributes") or {}
    return {NAME: attributes.get("name"), URL: attributes.get("url")}


def archived_at(
    config: dict | None,
    date_created: Callable[[], str | None],
    last_modified: datetime | None,
) -> datetime | None:
    """Same order as dcpy.connectors.edm.recipes.get_archive_date: the config's run timestamp,
    then config.json's date-created metadata (only looked up if needed), then its LastModified."""
    if config and (timestamp := archive_timestamp_from_config(config)):
        return timestamp
    if created := date_created():
        return datetime.fromisoformat(created)
    return last_modified


def location(datastore: IngestDatastore, key: str, version: str) -> str:
    root = str(datastore.storage.storage.root_path).rstrip("/")
    return f"{root}/{key}/{version}/"


def row_for_archive(
    *,
    store: str,
    location: str | None,
    key: str,
    version: str,
    config: dict,
    files: Iterable[str],
    is_latest: bool,
    archived: datetime | None = None,
    config_modified: datetime | None = None,
) -> VersionRow:
    archived = archived or archive_timestamp_from_config(config)
    details = details_from_config(config)
    details[ARCHIVED_AT_ISO] = _iso(archived)
    if config_modified:
        details[CONFIG_MODIFIED] = _iso(config_modified)
    return VersionRow(
        domain=DOMAIN,
        store=store,
        key=key,
        version=version,
        location=location,
        file_types=file_types(files),
        archived_at=archived,
        is_latest=is_latest,
        details=details,
    )


@dataclass
class SweepStats:
    versions: int = 0
    configs_read: int = 0
    latest_read: int = 0
    errors: list[str] = field(default_factory=list)


def sweep(
    datastore: IngestDatastore,
    store: str,
    previous: Iterable[VersionRow] = (),
    *,
    max_workers: int = MAX_WORKERS,
) -> tuple[list[VersionRow], SweepStats]:
    """Every version in the datastore as a catalog row, from one listing. `previous` (the
    catalog's current rows for this store) saves rereading unchanged config.json files."""
    stats = SweepStats()
    known = {(r.key, r.version): r for r in previous}
    known_latest = {
        r.key: (r.version, r.details.get(LATEST_CONFIG_MODIFIED))
        for r in known.values()
        if r.is_latest
    }
    grouped = group_listing(datastore.storage.list_files(""))
    storage = datastore.storage

    def read_config(path: str) -> dict:
        return json.loads(storage.read_bytes(path))

    # What needs reading: new or changed version configs, changed latest configs.
    to_read: list[tuple[str, str]] = []
    latest_to_read: list[str] = []
    for key, folders in grouped.items():
        for folder, files in folders.items():
            if not is_version_folder(folder, files):
                continue
            stats.versions += 1
            prior = known.get((key, folder))
            modified = _iso(files[config_filename])
            if not (
                prior
                and modified
                and prior.details.get(CONFIG_MODIFIED) == modified
                and CONFIG_ERROR not in prior.details
            ):
                to_read.append((key, folder))
        latest_files = folders.get(LATEST, {})
        if config_filename in latest_files:
            modified = _iso(latest_files[config_filename])
            prior_latest = known_latest.get(key)
            if not (modified and prior_latest and prior_latest[1] == modified):
                latest_to_read.append(key)

    def read_version(item: tuple[str, str]) -> tuple[tuple[str, str], dict | Exception]:
        key, version = item
        try:
            return item, read_config(f"{key}/{version}/{config_filename}")
        except Exception as e:  # recorded on the row, not fatal to the sweep
            return item, e

    def read_latest(key: str) -> tuple[str, str | Exception]:
        try:
            config = read_config(f"{key}/{LATEST}/{config_filename}")
            return key, SparseConfig(**config).version
        except Exception as e:
            return key, e

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        configs = dict(pool.map(read_version, to_read))
        latest_read = dict(pool.map(read_latest, latest_to_read))
    stats.configs_read = len(configs)
    stats.latest_read = len(latest_read)

    latest: dict[str, tuple[str, str | None]] = {}
    for key, folders in grouped.items():
        modified = _iso(folders.get(LATEST, {}).get(config_filename))
        if key in latest_read:
            result = latest_read[key]
            if isinstance(result, Exception):
                stats.errors.append(f"{key}/{LATEST}: {result}")
                continue
            latest[key] = (result, modified)
        elif key in known_latest and LATEST in folders:
            latest[key] = (known_latest[key][0], known_latest[key][1])

    def date_created(key: str, version: str) -> Callable[[], str | None]:
        def lookup() -> str | None:
            try:
                metadata = storage.get_metadata(f"{key}/{version}/{config_filename}")
                return metadata.get(s3_metadata.DATE_CREATED)
            except Exception:
                return None

        return lookup

    rows: list[VersionRow] = []
    for key, folders in grouped.items():
        latest_version, latest_modified = latest.get(key, (None, None))
        for version, files in folders.items():
            if not is_version_folder(version, files):
                continue
            config_modified = files[config_filename]
            is_latest = version == latest_version
            if (key, version) in configs:
                config = configs[(key, version)]
                if isinstance(config, Exception):
                    stats.errors.append(f"{key}/{version}: {config}")
                    row = row_for_archive(
                        store=store,
                        location=location(datastore, key, version),
                        key=key,
                        version=version,
                        config={},
                        files=files,
                        is_latest=is_latest,
                        archived=config_modified,
                    )
                    row.details[CONFIG_ERROR] = str(config)
                else:
                    row = row_for_archive(
                        store=store,
                        location=location(datastore, key, version),
                        key=key,
                        version=version,
                        config=config,
                        files=files,
                        is_latest=is_latest,
                        archived=archived_at(
                            config, date_created(key, version), config_modified
                        ),
                        config_modified=config_modified,
                    )
            else:
                prior = known[(key, version)]
                row = VersionRow(
                    domain=DOMAIN,
                    store=store,
                    key=key,
                    version=version,
                    location=location(datastore, key, version),
                    file_types=file_types(files),
                    archived_at=prior.archived_at,
                    is_latest=is_latest,
                    details={
                        k: v
                        for k, v in prior.details.items()
                        if k != LATEST_CONFIG_MODIFIED
                    },
                )
            if is_latest and latest_modified:
                row.details[LATEST_CONFIG_MODIFIED] = latest_modified
            rows.append(row)
    return rows, stats


@dataclass
class RefreshResult:
    store: str
    counts: WriteCounts
    stats: SweepStats
    seconds: float


def refresh_store(
    datastore: IngestDatastore, store: str, *, conn_url: str | None = None
) -> RefreshResult:
    """Sweep one ingest datastore and make the catalog's `ingest` rows for it match."""
    start = time.monotonic()
    previous = query(DOMAIN, store=store, conn_url=conn_url)
    rows, stats = sweep(datastore, store, previous)
    counts = sync_store(DOMAIN, store, rows, conn_url=conn_url)
    seconds = time.monotonic() - start
    logger.info(
        f"Catalog: {store}: {stats.versions} version(s), {counts.added} added, "
        f"{counts.updated} updated, {counts.removed} removed, {stats.configs_read} config(s) "
        f"read, {len(stats.errors)} error(s), {seconds:.1f}s"
    )
    return RefreshResult(store=store, counts=counts, stats=stats, seconds=seconds)


def record_archive(
    datastore: IngestDatastore,
    store: str,
    *,
    key: str,
    version: str,
    config: dict,
    files: Iterable[str],
    latest: bool,
    conn_url: str | None = None,
) -> None:
    """Write through one archive ingest just pushed (and, with `latest`, pointed latest at)."""
    row = row_for_archive(
        store=store,
        location=location(datastore, key, version),
        key=key,
        version=version,
        config=config,
        files=files,
        is_latest=False,
    )
    # Pushing without latest doesn't move latest, so an existing row keeps its flag.
    upsert([row], update_is_latest=False, conn_url=conn_url)
    if latest:
        mark_latest(store, key=key, version=version, conn_url=conn_url)


def mark_latest(
    store: str, *, key: str, version: str, conn_url: str | None = None
) -> bool:
    """Latest now points at an archive that's already there. False if it has no row yet."""
    return set_latest(DOMAIN, store, key, version, conn_url=conn_url)
