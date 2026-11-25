"""Copy data from DigitalOcean Spaces into Azure Blob, through the versitygw S3 gateway, remapping
the layout on the way. See "Migrating data from DigitalOcean" in apps/docker/README.md.

Dry run by default: lists both ends and prints what would be copied. `--execute` copies.

Rerun it to keep Azure current. Version folders never change once written, so one whose marker
(config.json; see Folder) is at the destination is skipped whole, and the marker is copied
last, so an interrupted folder has none and is redone. latest/ is replaced when DigitalOcean's
is newer. Presence comes from the version catalog when DCPY_CATALOG_PG_URL answers, otherwise
from listing the destination; the plan is the same either way.

Source (DigitalOcean): AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
Destination (gateway): AZ_S3_ENDPOINT, AZ_S3_ACCESS_KEY_ID, AZ_S3_SECRET_ACCESS_KEY,
and AZ_S3_CA_BUNDLE for a gateway certificate from a private CA.
"""

import fnmatch
import json
import os
import re
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import pytz
import typer
import yaml
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from dcpy.utils import s3, s3_metadata
from dcpy.utils.logging import logger

REPO_ROOT = Path(__file__).parent.parent.parent
DEFAULT_TEMPLATES_DIR = REPO_ROOT / "ingest_templates"

# Stamped on every copied object: the DigitalOcean ETag, so a later `--verify` can tell the
# copy matches its source (gateway ETags aren't MD5s, and multipart ETags never are).
SOURCE_ETAG = "source_etag"

DEFAULT_RECIPES_ROOT_FOLDER = "ingest"
DEFAULT_INBOX_ROOT_FOLDER = "inbox"

AreaName = Literal["datasets", "raw", "private", "inbox", "recipes_inbox", "publishing"]
ALL_AREAS: tuple[AreaName, ...] = (
    "datasets",
    "raw",
    "private",
    "inbox",
    "recipes_inbox",
    "publishing",
)


@dataclass(frozen=True)
class Buckets:
    source_recipes: str = "edm-recipes"
    source_private: str = "edm-private"
    source_publishing: str = "edm-publishing"
    dest_recipes: str = "edm-recipes"
    dest_publishing: str = "edm-publishing"


@dataclass(frozen=True)
class Area:
    """One row of the mapping: everything under `source_bucket/source_prefix` goes to
    `dest_bucket/dest_prefix` with the rest of the key unchanged.

    kind:
      versioned  <id>/<version>/...; filters by id, --latest-only, --versions-since
      inbox      files read by ingest templates with an `s3` source on this bucket
      plain      <id>/...; filters by id (the product, for publishing)
    """

    name: AreaName
    source_bucket: str
    source_prefix: str
    dest_bucket: str
    dest_prefix: str
    kind: Literal["versioned", "inbox", "plain"]
    # Keys under these prefixes belong to another area.
    exclude_prefixes: tuple[str, ...] = ()
    # inbox: the bucket name ingest templates give for these files (always the DigitalOcean
    # one, whatever the source bucket is called here).
    template_bucket: str = ""


def _folder(root: str) -> str:
    root = root.strip().strip("/")
    return f"{root}/" if root else ""


def mapping(
    buckets: Buckets, recipes_root_folder: str, inbox_root_folder: str
) -> dict[AreaName, Area]:
    """The DigitalOcean -> Azure layout. Change it here."""
    ingest = _folder(recipes_root_folder)
    inbox = _folder(inbox_root_folder)
    rows = [
        # edm-recipes/datasets/<id>/<version>/  ->  edm-recipes/ingest/datasets/<id>/<version>/
        Area("datasets", buckets.source_recipes, "datasets/",
             buckets.dest_recipes, f"{ingest}datasets/", "versioned"),
        # edm-recipes/raw/<id>/<timestamp>/  ->  edm-recipes/ingest/raw/<id>/<timestamp>/
        Area("raw", buckets.source_recipes, "raw/",
             buckets.dest_recipes, f"{ingest}raw/", "versioned"),
        # edm-private/datasets/<id>/<version>/  ->  edm-recipes/ingest/datasets/<id>/<version>/
        Area("private", buckets.source_private, "datasets/",
             buckets.dest_recipes, f"{ingest}datasets/", "versioned"),
        # edm-private/<key read by a template>  ->  edm-recipes/inbox/<key>
        Area("inbox", buckets.source_private, "",
             buckets.dest_recipes, inbox, "inbox", exclude_prefixes=("datasets/",),
             template_bucket="edm-private"),
        # edm-recipes/inbox/<key>  ->  edm-recipes/inbox/<key> (templates name it directly)
        Area("recipes_inbox", buckets.source_recipes, "inbox/",
             buckets.dest_recipes, "inbox/", "inbox", template_bucket="edm-recipes"),
        # edm-publishing/...  ->  edm-publishing/... unchanged
        Area("publishing", buckets.source_publishing, "",
             buckets.dest_publishing, "", "plain"),
    ]  # fmt: skip
    return {row.name: row for row in rows}


def dest_key(area: Area, source_key: str) -> str:
    if not source_key.startswith(area.source_prefix):
        raise ValueError(f"{source_key} is not under {area.source_prefix!r}")
    return area.dest_prefix + source_key[len(area.source_prefix) :]


def recipes_root_folder_from_env() -> str:
    """Matches the az.* connectors: AZ_RECIPES_BUCKET_ROOT_FOLDER, then
    RECIPES_BUCKET_ROOT_FOLDER. Empty or unset means `ingest`: DigitalOcean's own env leaves
    them empty, and the Azure layout never uses the bucket root."""
    return (
        os.environ.get("AZ_RECIPES_BUCKET_ROOT_FOLDER")
        or os.environ.get("RECIPES_BUCKET_ROOT_FOLDER")
        or DEFAULT_RECIPES_ROOT_FOLDER
    )


def inbox_root_folder_from_env() -> str:
    return os.environ.get("INBOX_ROOT_FOLDER") or DEFAULT_INBOX_ROOT_FOLDER


# --- inbox files: what ingest templates read -------------------------------------------------


@dataclass(frozen=True)
class InboxSource:
    template_id: str
    bucket: str
    key_template: str

    @property
    def static_prefix(self) -> str:
        return self.key_template.split("{{")[0]

    def matches(self, key: str) -> bool:
        parts = re.split(r"\{\{.*?\}\}", self.key_template)
        return re.fullmatch(".+?".join(re.escape(p) for p in parts), key) is not None


_S3_SOURCE = re.compile(
    r"type:\s*s3\s*\n\s*bucket:\s*(?P<bucket>\S+)\s*\n\s*key:\s*(?P<key>.+)"
)


def _walk_s3_sources(node, found: list[tuple[str, str]]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "s3" and "bucket" in node and "key" in node:
            found.append((str(node["bucket"]), str(node["key"])))
        for value in node.values():
            _walk_s3_sources(value, found)
    elif isinstance(node, list):
        for value in node:
            _walk_s3_sources(value, found)


def inbox_sources(templates_dir: Path) -> list[InboxSource]:
    """Every `s3` source in the ingest templates. Templates that aren't valid YAML before
    jinja rendering are read with a regex instead."""
    sources = []
    for path in sorted(templates_dir.glob("*.yml")):
        text = path.read_text()
        found: list[tuple[str, str]] = []
        try:
            _walk_s3_sources(yaml.safe_load(text), found)
        except yaml.YAMLError:
            found = [
                (m["bucket"], m["key"].strip().strip("'\""))
                for m in _S3_SOURCE.finditer(text)
            ]
        sources += [InboxSource(path.stem, bucket, key) for bucket, key in found]
    return sources


# --- selection -------------------------------------------------------------------------------


@dataclass
class Selection:
    areas: tuple[AreaName, ...] = ALL_AREAS
    # Globs on the dataset id (template id for inbox areas, product for publishing).
    datasets: tuple[str, ...] = ()
    latest_only: bool = False
    versions_since: date | None = None
    include_unmatched_inbox: bool = False

    def wants(self, dataset_id: str) -> bool:
        return not self.datasets or any(
            fnmatch.fnmatchcase(dataset_id, p) for p in self.datasets
        )

    @property
    def literal_datasets(self) -> bool:
        return bool(self.datasets) and not any(
            set(p) & set("*?[") for p in self.datasets
        )


def read_datasets_file(path: Path) -> list[str]:
    """One id or glob per line; blank lines and # comments are ignored."""
    lines = (line.split("#")[0].strip() for line in path.read_text().splitlines())
    return [line for line in lines if line]


@dataclass(frozen=True)
class SourceObject:
    area: AreaName
    key: str
    size: int
    etag: str
    last_modified: datetime
    dataset_id: str
    version: str = ""


ItemStatus = Literal["copy", "present", "conflict", "unverified", "kept"]


def _list(client, bucket: str, prefix: str) -> Iterator[dict]:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        yield from page.get("Contents", [])


def _etag(raw: str | None) -> str:
    return (raw or "").strip('"')


def _top_level(client, area: Area) -> list[str]:
    """The area's folders one level down, minus excluded ones (edm-private's datasets/), so
    listing them skips what belongs to another area. Objects right at the top come back as
    their own keys, which list as themselves."""
    response = client.list_objects_v2(
        Bucket=area.source_bucket, Prefix=area.source_prefix, Delimiter="/"
    )
    if response.get("IsTruncated"):
        return [area.source_prefix]
    found = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
    found += [o["Key"] for o in response.get("Contents", [])]
    return sorted(
        p for p in found if not any(p.startswith(x) for x in area.exclude_prefixes)
    )


def _id_and_version(area: Area, key: str) -> tuple[str, str]:
    parts = key[len(area.source_prefix) :].split("/")
    dataset_id = parts[0] if len(parts) > 1 else ""
    version = parts[1] if area.kind == "versioned" and len(parts) > 2 else ""
    return dataset_id, version


@dataclass
class Unmatched:
    """Objects under an inbox area's prefix that no template reads."""

    count: int = 0
    size: int = 0
    sample: list[str] = field(default_factory=list)


def list_area(
    client,
    area: Area,
    selection: Selection,
    sources: list[InboxSource],
    unmatched: Unmatched | None = None,
) -> Iterator[SourceObject]:
    def obj(o: dict, dataset_id: str, version: str = "") -> SourceObject:
        return SourceObject(
            area=area.name,
            key=o["Key"],
            size=o["Size"],
            etag=_etag(o.get("ETag")),
            last_modified=o["LastModified"],
            dataset_id=dataset_id,
            version=version,
        )

    def excluded(key: str) -> bool:
        return key.endswith("/") or any(
            key.startswith(p) for p in area.exclude_prefixes
        )

    if area.kind == "inbox":
        templates = [
            s
            for s in sources
            if s.bucket == area.template_bucket
            and s.static_prefix.startswith(area.source_prefix)
        ]
        everything = selection.include_unmatched_inbox and not selection.datasets
        if everything or unmatched is not None and not selection.datasets:
            prefixes = _top_level(client, area)
        else:
            prefixes = sorted(
                {s.static_prefix for s in templates if selection.wants(s.template_id)}
            )
            # One prefix inside another would list the same objects twice.
            prefixes = [
                p
                for p in prefixes
                if not any(p != q and p.startswith(q) for q in prefixes)
            ]
        for prefix in prefixes:
            for o in _list(client, area.source_bucket, prefix):
                if excluded(o["Key"]):
                    continue
                readers = [s.template_id for s in templates if s.matches(o["Key"])]
                wanted = [t for t in readers if selection.wants(t)]
                if wanted:
                    yield obj(o, wanted[0])
                elif not readers:
                    if everything:
                        yield obj(o, "")
                    elif unmatched is not None:
                        unmatched.count += 1
                        unmatched.size += o["Size"]
                        if len(unmatched.sample) < 10:
                            unmatched.sample.append(o["Key"])
        return

    if selection.literal_datasets:
        prefixes = [
            f"{area.source_prefix}{d}/" for d in sorted(set(selection.datasets))
        ]
    else:
        prefixes = [area.source_prefix]
    for prefix in prefixes:
        for o in _list(client, area.source_bucket, prefix):
            if excluded(o["Key"]):
                continue
            dataset_id, version = _id_and_version(area, o["Key"])
            if dataset_id and selection.wants(dataset_id):
                yield obj(o, dataset_id, version)


def _latest_version(client, area: Area, dataset_id: str) -> str | None:
    """The version `<id>/latest/` points at, from its config.json (ingest and library)."""
    from dcpy.connectors.edm.models import SparseConfig

    key = f"{area.source_prefix}{dataset_id}/latest/config.json"
    try:
        body = client.get_object(Bucket=area.source_bucket, Key=key)["Body"].read()
        return SparseConfig(**json.loads(body)).version
    except Exception as e:
        logger.warning(f"No version for {area.source_bucket}/{key}: {e}")
        return None


def filter_versions(
    client, area: Area, objects: list[SourceObject], selection: Selection, workers: int
) -> list[SourceObject]:
    """--latest-only: each dataset's `latest/` plus the version it points at (raw has no
    `latest/`: its newest timestamp folder). --versions-since: version folders with an object
    written on or after that date (DigitalOcean's LastModified)."""
    if area.kind != "versioned" or not (
        selection.latest_only or selection.versions_since
    ):
        return objects
    by_version: dict[tuple[str, str], list[SourceObject]] = defaultdict(list)
    for o in objects:
        by_version[(o.dataset_id, o.version)].append(o)

    keep = {k for k in by_version if k[1]}
    if selection.versions_since:
        since = selection.versions_since
        keep = {
            k
            for k in keep
            if max(o.last_modified for o in by_version[k]).date() >= since
        }
    if selection.latest_only:
        ids = sorted({d for d, _ in by_version})
        latest: dict[str, str | None]
        if area.name == "raw":
            latest = {
                d: max((v for i, v in by_version if i == d and v), default="")
                for d in ids
            }
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                versions = pool.map(lambda d: _latest_version(client, area, d), ids)
                latest = dict(zip(ids, versions))
        keep = {
            (d, v)
            for d, v in keep
            if v == latest.get(d) or (area.name != "raw" and v == "latest")
        }
    return [o for k in sorted(keep) for o in by_version[k]]


# --- planning --------------------------------------------------------------------------------

CONFIG_JSON = "config.json"
# The file copied last in a publishing folder, the first of these it has (else its last key).
PUBLISHING_MARKERS = ("build_metadata.json", "version.txt")

FolderKind = Literal["version", "latest", "publish"]
FolderStatus = Literal["present", "copy", "conflict", "replace", "kept"]


@dataclass(eq=False)
class Folder:
    """Files copied as a unit, with one of them, the marker, copied last: after every other file
    in the folder is at the destination. So a marker at the destination means a complete
    folder, and an interrupted copy leaves none, which the next run redoes.

    kind:
      version  <id>/<version>/ in datasets, raw, private, with a config.json (the marker).
               Never changes once written: present at the destination means skipped whole.
      publish  <product>/publish/<version>/ and <product>/draft/<version>/<revision>/. Same.
      latest   <id>/latest/. Changes: replaced wholesale when DigitalOcean's is newer.

    status:
      present   at the destination, same as DigitalOcean: skipped
      copy      missing or incomplete at the destination
      conflict  at the destination but different (never overwritten), or incomplete with a
                file that differs (marker held back)
      replace   latest/ that DigitalOcean has newer: copied, stale files deleted
      kept      latest/ left alone, see reason
    """

    area: AreaName
    dataset_id: str
    source_prefix: str
    dest_prefix: str
    kind: FolderKind
    marker: str  # source key
    items: list["Item"] = field(default_factory=list, repr=False)
    status: FolderStatus = "copy"
    reason: str = ""
    # The marker was found at the destination.
    complete: bool = False
    # latest/ replace: destination keys DigitalOcean's latest/ doesn't have, deleted before the
    # marker goes up.
    deletions: list[str] = field(default_factory=list)
    # Set by execute: everything copied, marker last.
    done: bool = False

    @property
    def marker_item(self) -> "Item":
        return next(i for i in self.items if i.source.key == self.marker)

    def set_items(self, status: ItemStatus) -> None:
        for i in self.items:
            i.status = status


@dataclass(eq=False)
class Item:
    source: SourceObject
    dest_bucket: str
    dest_key: str
    status: ItemStatus = "copy"
    folder: Folder | None = field(default=None, repr=False)

    @property
    def is_marker(self) -> bool:
        return self.folder is not None and self.folder.marker == self.source.key


@dataclass
class Plan:
    items: list[Item] = field(default_factory=list)
    folders: list[Folder] = field(default_factory=list)
    # area -> how destination folders were found: "catalog" or "listing"
    presence: dict[str, str] = field(default_factory=dict)


def _folder_of(area: Area, key: str) -> tuple[str, FolderKind] | None:
    parts = key[len(area.source_prefix) :].split("/")
    if area.kind == "versioned":
        if len(parts) < 3 or not parts[0] or not parts[1]:
            return None
        kind: FolderKind = (
            "latest" if parts[1] == "latest" and area.name != "raw" else "version"
        )
        return f"{area.source_prefix}{parts[0]}/{parts[1]}/", kind
    if area.kind == "plain":
        if len(parts) >= 4 and parts[1] == "publish" and parts[2] != "latest":
            return f"{area.source_prefix}{'/'.join(parts[:3])}/", "publish"
        if len(parts) >= 5 and parts[1] == "draft":
            return f"{area.source_prefix}{'/'.join(parts[:4])}/", "publish"
    return None


def _marker(prefix: str, kind: FolderKind, keys: set[str]) -> str | None:
    if kind in ("version", "latest"):
        return prefix + CONFIG_JSON if prefix + CONFIG_JSON in keys else None
    for name in PUBLISHING_MARKERS:
        if prefix + name in keys:
            return prefix + name
    return max(keys)


def group_folders(area: Area, items: list[Item]) -> tuple[list[Folder], list[Item]]:
    """(folders, loose items). A dataset version folder without a config.json at DigitalOcean
    (some library-era ones) has no marker: its files are compared one by one."""
    grouped: dict[tuple[str, FolderKind], list[Item]] = defaultdict(list)
    loose: list[Item] = []
    for i in items:
        found = _folder_of(area, i.source.key)
        if found is None:
            loose.append(i)
        else:
            grouped[found].append(i)
    folders = []
    for (prefix, kind), members in grouped.items():
        marker = _marker(prefix, kind, {i.source.key for i in members})
        if marker is None:
            loose += members
            continue
        folder = Folder(
            area=area.name,
            dataset_id=members[0].source.dataset_id,
            source_prefix=prefix,
            dest_prefix=dest_key(area, prefix),
            kind=kind,
            marker=marker,
            items=members,
        )
        for i in members:
            i.folder = folder
        folders.append(folder)
    return folders, loose


def _dest_index(
    client, bucket: str, prefixes: Iterable[str], workers: int
) -> dict[str, int]:
    def listing(prefix: str) -> dict[str, int]:
        return {o["Key"]: o["Size"] for o in _list(client, bucket, prefix)}

    index: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for part in pool.map(listing, sorted(set(prefixes))):
            index.update(part)
    return index


def _common_prefix(keys: list[str]) -> str:
    prefix = os.path.commonprefix(keys)
    return prefix[: prefix.rfind("/") + 1]


def _head(client, bucket: str, key: str) -> dict | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in s3.MISSING_OBJECT_CODES:
            return None
        raise


def _heads(
    client, bucket: str, keys: list[str], workers: int
) -> dict[str, dict | None]:
    if not keys:
        return {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(zip(keys, pool.map(lambda k: _head(client, bucket, k), keys)))


def _stored_etag(head: dict) -> str | None:
    return s3_metadata.for_read(head.get("Metadata")).get(SOURCE_ETAG)


def _read(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


# --- the version catalog ---------------------------------------------------------------------


def catalog_covers(area: Area) -> bool:
    """The catalog has processed dataset versions (`ingest` domain), not raw or the rest."""
    return area.kind == "versioned" and area.name != "raw"


def load_catalog(dest_bucket: str, dest_prefixes: Iterable[str]) -> dict | None:
    """The catalog's version rows under these destination folders, by location
    (`s3://<bucket>/<prefix><id>/<version>/`). None, so the destination is listed instead, when
    DCPY_CATALOG_PG_URL isn't set or the catalog doesn't answer."""
    try:
        from dcpy.services import catalog
        from dcpy.services.catalog import ingest_versions
    except ImportError as e:
        logger.info(f"Version catalog unavailable ({e}); listing the destination.")
        return None
    if not catalog.is_configured():
        logger.info(
            f"{catalog.CATALOG_PG_URL_ENV_VAR} not set; listing the destination."
        )
        return None
    try:
        rows = catalog.query(ingest_versions.DOMAIN)
    except Exception as e:
        logger.warning(f"Version catalog unreachable ({e}); listing the destination.")
        return None
    roots = tuple(f"s3://{dest_bucket}/{p}" for p in dest_prefixes)
    found = {r.location: r for r in rows if r.location and r.location.startswith(roots)}
    logger.info(f"Version catalog: {len(found)} version(s) under {', '.join(roots)}")
    return found


def _catalog_archived_at(catalog: dict | None, location: str) -> datetime | None:
    row = catalog.get(location) if catalog else None
    if row is None:
        return None
    iso = (row.details or {}).get("archived_at_iso")
    return datetime.fromisoformat(iso) if iso else row.archived_at


# --- latest/ ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class ConfigInfo:
    version: str
    archived_at: datetime


def read_config(client, bucket: str, key: str) -> ConfigInfo | None:
    """A config.json's version and archive date. None when there's no such object.

    The archive date resolves as dcpy.connectors.edm.recipes.get_archive_date does: the run
    timestamp in the config (ingest or library), then its date-created metadata, then its
    LastModified."""
    from dcpy.connectors.edm.models import SparseConfig, archive_timestamp_from_config

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in s3.MISSING_OBJECT_CODES:
            return None
        raise
    config = json.loads(obj["Body"].read())
    created = s3_metadata.for_read(obj.get("Metadata")).get(s3_metadata.DATE_CREATED)
    archived = (
        archive_timestamp_from_config(config)
        or (datetime.fromisoformat(created) if created else None)
        or obj["LastModified"]
    )
    return ConfigInfo(SparseConfig(**config).version, archived)


def _aware(dt: datetime) -> datetime:
    ny = pytz.timezone("America/New_York")
    return (dt if dt.tzinfo else ny.localize(dt)).astimezone(ny)


def _plan_latest(source, dest, area: Area, folder: Folder, catalog, workers: int):
    """Equal versions: skipped. DigitalOcean's newer: replaced. Azure's newer (it ingested on
    its own): kept."""
    try:
        do = read_config(source, area.source_bucket, folder.marker)
        az = read_config(dest, area.dest_bucket, dest_key(area, folder.marker))
    except Exception as e:
        folder.status, folder.reason = "kept", f"latest/config.json unreadable: {e}"
        folder.set_items("kept")
        return
    if do is None:  # gone since the listing
        folder.status, folder.reason = "kept", "no latest/config.json at DigitalOcean"
        folder.set_items("kept")
        return
    if az is not None and az.version == do.version:
        folder.status, folder.reason = "present", f"same version {do.version}"
        folder.complete = True
        folder.set_items("present")
        return
    if az is not None:
        location = f"s3://{area.dest_bucket}/{area.dest_prefix}{folder.dataset_id}/{az.version}/"
        az_date = _aware(_catalog_archived_at(catalog, location) or az.archived_at)
        do_date = _aware(do.archived_at)
        if do_date <= az_date:
            folder.status = "kept"
            folder.reason = (
                f"azure latest newer: {az.version} ({az_date:%Y-%m-%d %H:%M}) vs "
                f"DigitalOcean {do.version} ({do_date:%Y-%m-%d %H:%M})"
            )
            folder.set_items("kept")
            return

    folder.status = "replace"
    folder.reason = f"{az.version if az else 'none'} -> {do.version}"
    listing = {
        o["Key"]: o["Size"] for o in _list(dest, area.dest_bucket, folder.dest_prefix)
    }
    wanted = {i.dest_key for i in folder.items}
    folder.deletions = sorted(k for k in listing if k not in wanted)
    # A file already there from an interrupted replace, by its stamped source ETag.
    same_size = [
        i
        for i in folder.items
        if not i.is_marker and listing.get(i.dest_key) == i.source.size
    ]
    heads = _heads(dest, area.dest_bucket, [i.dest_key for i in same_size], workers)
    for i in folder.items:
        head = heads.get(i.dest_key)
        done = head is not None and _stored_etag(head) == i.source.etag
        i.status = "present" if done else "copy"


# --- per area --------------------------------------------------------------------------------


def _plan_incomplete(folder: Folder, index: dict[str, int], overwrite: bool) -> None:
    """No marker at the destination: resume file by file, marker last."""
    for i in folder.items:
        if i.is_marker or i.dest_key not in index:
            i.status = "copy"
        elif index[i.dest_key] == i.source.size:
            i.status = "present"
        else:
            i.status = "copy" if overwrite else "conflict"
    held = any(i.status == "conflict" for i in folder.items)
    folder.status = "conflict" if held else "copy"
    if held:
        # Writing the marker would vouch for a folder with a file that differs.
        folder.marker_item.status = "conflict"
        folder.reason = "incomplete at destination, and a file there differs"
    elif any(i.status == "present" for i in folder.items):
        folder.reason = "incomplete at destination, resuming"


def _plan_loose(
    dest, area: Area, loose: list[Item], index: dict[str, int], overwrite: bool, workers
) -> None:
    """Files outside folders. Inbox and publishing ones can change at DigitalOcean, so present
    means same size and the destination's stamped source ETag is DigitalOcean's (a HEAD each);
    a stamped ETag that's different means DigitalOcean changed it since: copied again. Without
    a stamp it was written some other way: a conflict. Elsewhere (library-era versions with no
    config.json), same size is present, as before."""
    strict = area.kind in ("inbox", "plain")
    heads = (
        _heads(dest, area.dest_bucket, [i.dest_key for i in loose if i.dest_key in index], workers)
        if strict
        else {}
    )  # fmt: skip
    for i in loose:
        if i.dest_key not in index:
            i.status = "copy"
            continue
        same_size = index[i.dest_key] == i.source.size
        if not strict:
            i.status = "present" if same_size else "conflict"
        elif (head := heads.get(i.dest_key)) is None:
            i.status = "copy"
        elif (stored := _stored_etag(head)) is None:
            i.status = "conflict"
        else:
            i.status = "present" if same_size and stored == i.source.etag else "copy"
        if i.status == "conflict" and overwrite:
            i.status = "copy"


def _plan_area(
    source,
    dest,
    area: Area,
    folders: list[Folder],
    loose: list[Item],
    catalog: dict | None,
    *,
    overwrite: bool,
    trust_catalog: bool,
    workers: int,
) -> None:
    fixed = [f for f in folders if f.kind != "latest"]

    def location(f: Folder) -> str:
        return f"s3://{area.dest_bucket}/{f.dest_prefix}"

    in_catalog = {f for f in fixed if catalog is not None and location(f) in catalog}

    # One listing of the destination per dataset folder, as close to the selection as the
    # keys allow. With the catalog, only for datasets with a folder it doesn't vouch for.
    keys_by_id: dict[str, list[str]] = defaultdict(list)
    for f in fixed:
        keys_by_id[f.dataset_id] += [i.dest_key for i in f.items]
    for i in loose:
        keys_by_id[i.source.dataset_id].append(i.dest_key)

    def index_for(ids: set[str]) -> dict[str, int]:
        prefixes = [_common_prefix(keys_by_id[d]) for d in ids if keys_by_id[d]]
        return _dest_index(dest, area.dest_bucket, prefixes, workers)

    if catalog is None:
        listed = set(keys_by_id)
    else:
        listed = {f.dataset_id for f in fixed if f not in in_catalog}
        listed |= {i.source.dataset_id for i in loose}
    index = index_for(listed)

    def marker_dest(f: Folder) -> str:
        return dest_key(area, f.marker)

    claimed = [f for f in fixed if f in in_catalog or marker_dest(f) in index]
    # One HEAD of each marker: confirms a catalog row, and catches a version that's at both
    # ends but different (Azure ingested it too). --trust-catalog skips it for catalog rows.
    check = [f for f in claimed if not (trust_catalog and f in in_catalog)]
    heads = _heads(dest, area.dest_bucket, [marker_dest(f) for f in check], workers)
    # No stamped source ETag: not copied by this script (Azure ingested it, say). Same size
    # proves little there, since two runs' configs differ only in timestamps, so compare the two
    # config.jsons' bytes: small, and rare.
    unstamped = [
        f
        for f in check
        if (head := heads[marker_dest(f)]) is not None and _stored_etag(head) is None
    ]

    def same_bytes(f: Folder) -> bool:
        return _read(source, area.source_bucket, f.marker) == _read(
            dest, area.dest_bucket, marker_dest(f)
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        identical = dict(zip(unstamped, pool.map(same_bytes, unstamped)))
    gone: set[str] = set()
    for f in claimed:
        if f in check:
            head = heads[marker_dest(f)]
            if head is None:
                gone.add(f.dataset_id)
                continue
            if f in identical:
                same = identical[f]
            else:
                same = _stored_etag(head) == f.marker_item.source.etag
            if not same:
                f.complete, f.status = True, "conflict"
                f.reason = "at both ends, config.json differs; not overwritten"
                f.set_items("conflict")
                continue
        f.complete, f.status = True, "present"
        f.set_items("present")
    # Catalog rows for folders that aren't there: list those datasets too.
    if gone - listed:
        index.update(index_for(gone - listed))
    for f in fixed:
        if not f.complete:
            _plan_incomplete(f, index, overwrite)
    latest = [f for f in folders if f.kind == "latest"]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(_plan_latest, source, dest, area, f, catalog, workers)
            for f in latest
        ]
        for future in futures:
            future.result()
    _plan_loose(dest, area, loose, index, overwrite, workers)


def _verify(dest, items: list[Item], workers: int) -> None:
    """Every present object's stamped source ETag, against DigitalOcean's. Missing (a folder
    an older run marked complete too early) means copy it."""

    def check(item: Item) -> None:
        head = _head(dest, item.dest_bucket, item.dest_key)
        if head is None:
            item.status = "copy"
            return
        stored = _stored_etag(head)
        if stored is None:
            item.status = "unverified"
        elif stored != item.source.etag:
            item.status = "conflict"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for future in as_completed(pool.submit(check, i) for i in items):
            future.result()


def plan(
    source,
    dest,
    areas: dict[AreaName, Area],
    selection: Selection,
    sources: list[InboxSource],
    *,
    overwrite: bool = False,
    verify: bool = False,
    workers: int = 8,
    unmatched: dict[AreaName, Unmatched] | None = None,
    catalog: dict | None = None,
    trust_catalog: bool = False,
) -> Plan:
    """What to copy: every selected source object, with what the destination already has.

    Version folders (and publishing's) are whole: present when their marker is at the
    destination, found from `catalog` (see load_catalog) where it covers the area, otherwise by
    listing; the plan is the same either way. A folder at both ends that differs is a
    conflict, never overwritten. latest/ follows _plan_latest. Other objects are compared one by
    one; `overwrite` replaces those that differ. With `verify`, every present object is checked
    against the source ETag stamped on it at copy time.
    """
    result = Plan()
    for name in selection.areas:
        area = areas[name]
        found = None
        if unmatched is not None and area.kind == "inbox":
            found = unmatched.setdefault(name, Unmatched())
        objects = list(list_area(source, area, selection, sources, found))
        objects = filter_versions(source, area, objects, selection, workers)
        logger.info(f"{name}: {len(objects)} objects selected in {area.source_bucket}")
        area_catalog = catalog if catalog_covers(area) else None
        result.presence[name] = "catalog" if area_catalog is not None else "listing"
        if not objects:
            continue
        items = [Item(o, area.dest_bucket, dest_key(area, o.key)) for o in objects]
        folders, loose = group_folders(area, items)
        _plan_area(
            source,
            dest,
            area,
            folders,
            loose,
            area_catalog,
            overwrite=overwrite,
            trust_catalog=trust_catalog,
            workers=workers,
        )
        result.items += items
        result.folders += folders

    if verify:
        _verify(dest, [i for i in result.items if i.status == "present"], workers)
        for f in result.folders:
            if f.status == "present" and any(i.status == "copy" for i in f.items):
                f.status, f.reason = "copy", "incomplete at destination (--verify)"
        if overwrite:
            for i in result.items:
                # Never inside a complete version folder.
                if i.status == "conflict" and (
                    i.folder is None or not i.folder.complete
                ):
                    i.status = "copy"
    return result


# --- copying ---------------------------------------------------------------------------------


def copy_metadata(source_metadata: dict, last_modified: datetime, etag: str) -> dict:
    """DigitalOcean's metadata in the Azure-safe form (date-created -> date_created). An
    object with no date_created gets its LastModified, so archive dates survive the copy."""
    metadata = s3_metadata.for_write(s3_metadata.for_read(source_metadata))
    if s3_metadata.DATE_CREATED not in metadata:
        metadata[s3_metadata.DATE_CREATED] = last_modified.astimezone(
            pytz.timezone("America/New_York")
        ).isoformat()
    metadata[SOURCE_ETAG] = etag
    return metadata


def transfer_config(part_size_mb: int, threads_per_file: int) -> TransferConfig:
    part = part_size_mb * 1024 * 1024
    config = TransferConfig(
        multipart_threshold=part,
        multipart_chunksize=part,
        max_concurrency=threads_per_file,
        use_threads=threads_per_file > 1,
    )
    # A DO body isn't seekable, so s3transfer buffers parts in memory: cap them per object.
    config.max_in_memory_upload_chunks = threads_per_file + 1
    return config


def copy_one(source, dest, areas, item: Item, config: TransferConfig, attempts=3):
    area = areas[item.source.area]
    for attempt in range(1, attempts + 1):
        try:
            obj = source.get_object(Bucket=area.source_bucket, Key=item.source.key)
            extra = {
                "Metadata": copy_metadata(
                    obj.get("Metadata", {}), obj["LastModified"], _etag(obj["ETag"])
                )
            }
            if obj.get("ContentType"):
                extra["ContentType"] = obj["ContentType"]
            # Streams: the body is read one part at a time, never whole.
            dest.upload_fileobj(
                obj["Body"],
                item.dest_bucket,
                item.dest_key,
                ExtraArgs=extra,
                Config=config,
            )
            return
        except ClientError as e:
            if e.response["Error"]["Code"] in s3.MISSING_OBJECT_CODES:
                raise
            if attempt == attempts:
                raise
            logger.warning(f"{item.source.key}: {e}; retrying ({attempt})")
            time.sleep(2**attempt)
        except Exception as e:
            if attempt == attempts:
                raise
            logger.warning(f"{item.source.key}: {e}; retrying ({attempt})")
            time.sleep(2**attempt)


def _delete(dest, bucket: str, keys: list[str]) -> None:
    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        response = dest.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        if errors := response.get("Errors"):
            raise RuntimeError(f"Deleting {len(errors)} object(s) failed: {errors[:3]}")


def finish_folder(source, dest, areas, folder: Folder, config: TransferConfig):
    """After the rest of the folder: latest/'s stale files go, then the marker goes up."""
    if folder.deletions:
        _delete(dest, folder.marker_item.dest_bucket, folder.deletions)
    copy_one(source, dest, areas, folder.marker_item, config)


class _Progress:
    def __init__(self, total_objects: int, total_bytes: int, every_seconds: float):
        self.total_objects, self.total_bytes = total_objects, total_bytes
        self.objects = self.bytes = 0
        self.start = self.last = time.monotonic()
        self.every = every_seconds
        self.lock = threading.Lock()

    def add(self, size: int) -> None:
        with self.lock:
            self.objects += 1
            self.bytes += size
            now = time.monotonic()
            if now - self.last >= self.every or self.objects == self.total_objects:
                self.last = now
                logger.info(self.line())

    def line(self) -> str:
        elapsed = max(time.monotonic() - self.start, 1e-6)
        rate = self.bytes / elapsed
        left = (self.total_bytes - self.bytes) / rate if rate else 0
        return (
            f"{self.objects}/{self.total_objects} objects, "
            f"{_size(self.bytes)}/{_size(self.total_bytes)}, "
            f"{_size(rate)}/s, about {_duration(left)} left"
        )


def execute(
    source,
    dest,
    areas,
    items: list[Item],
    *,
    workers: int,
    part_size_mb: int,
    threads_per_file: int = 4,
    log_every_seconds: float = 30,
) -> list[tuple[Item, Exception]]:
    """Copy every `copy` item. A folder's marker is only submitted once the folder's other
    files have all copied, so concurrency can't put it up first; a failed file holds it back."""
    todo = [i for i in items if i.status == "copy"]
    markers = {i.folder: i for i in todo if i.is_marker and i.folder is not None}
    files = [i for i in todo if not i.is_marker]
    # Big files first: they set the run's length, and small ones fill in around them.
    files.sort(key=lambda i: -i.source.size)
    waiting: Counter = Counter(i.folder for i in files if i.folder in markers)
    blocked: set[Folder] = set()
    config = transfer_config(part_size_mb, threads_per_file)
    progress = _Progress(len(todo), sum(i.source.size for i in todo), log_every_seconds)
    failures: list[tuple[Item, Exception]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures: dict = {}

        def finish(folder: Folder) -> None:
            future = pool.submit(finish_folder, source, dest, areas, folder, config)
            futures[future] = markers[folder]

        for i in files:
            futures[pool.submit(copy_one, source, dest, areas, i, config)] = i
        for folder in markers:
            if not waiting[folder]:
                finish(folder)
        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                item = futures.pop(future)
                folder = item.folder
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Failed: {item.source.key} -> {item.dest_key}: {e}")
                    failures.append((item, e))
                    if folder is not None:
                        blocked.add(folder)
                    continue
                progress.add(item.source.size)
                if folder is None:
                    continue
                if item.is_marker:
                    folder.done = True
                elif folder in markers:
                    waiting[folder] -= 1
                    if not waiting[folder] and folder not in blocked:
                        finish(folder)
    for folder in {i.folder for i in files if i.folder is not None} - set(markers):
        folder.done = folder not in blocked
    logger.info(f"Done: {progress.line()}; {len(failures)} failed")
    return failures


# --- reporting -------------------------------------------------------------------------------


def _size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def _duration(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 90 * 60:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def _folder_line(folders: list[Folder]) -> str:
    fixed = [f for f in folders if f.kind != "latest"]
    latest = [f for f in folders if f.kind == "latest"]
    cells = []
    if fixed:
        to_copy = [f for f in fixed if f.status == "copy"]
        size = sum(
            i.source.size for f in to_copy for i in f.items if i.status == "copy"
        )
        cells.append(
            f"folders: {sum(f.status == 'present' for f in fixed)} present (skipped), "
            f"{len(to_copy)} to copy ({_size(size)}), "
            f"{sum(f.status == 'conflict' for f in fixed)} conflict"
        )
    if latest:
        deletions = sum(len(f.deletions) for f in latest)
        stale = f" ({deletions} stale file(s) to delete)" if deletions else ""
        cells.append(
            f"latest/: {sum(f.status == 'present' for f in latest)} same, "
            f"{sum(f.status == 'replace' for f in latest)} to replace{stale}, "
            f"{sum(f.status == 'kept' for f in latest)} kept"
        )
    return "; ".join(cells)


def summary(
    result: Plan,
    areas: dict[AreaName, Area],
    unmatched: dict[AreaName, Unmatched] | None = None,
    top: int = 25,
    samples: int = 3,
) -> str:
    items = result.items
    lines = []
    statuses = ("copy", "present", "conflict", "unverified", "kept")
    by_area: dict[str, list[Item]] = defaultdict(list)
    for i in items:
        by_area[i.source.area].append(i)
    folders_by_area: dict[str, list[Folder]] = defaultdict(list)
    for f in result.folders:
        folders_by_area[f.area].append(f)

    def counts(group: list[Item]) -> str:
        cells = []
        for status in statuses:
            matching = [i for i in group if i.status == status]
            if matching or status in ("copy", "present"):
                size = sum(i.source.size for i in matching)
                cells.append(f"{status} {len(matching)} ({_size(size)})")
        return ", ".join(cells)

    if result.presence:
        lines.append(
            "Destination presence from: "
            + ", ".join(f"{name} {how}" for name, how in result.presence.items())
        )
    lines.append("By area:")
    for name, group in by_area.items():
        a = areas[name]  # type: ignore[index]
        lines.append(
            f"  {name:<14} {a.source_bucket}/{a.source_prefix} -> "
            f"{a.dest_bucket}/{a.dest_prefix}: {counts(group)}"
        )
        if folders_by_area[name]:
            lines.append(f"  {'':<14} {_folder_line(folders_by_area[name])}")
        if unmatched and (u := unmatched.get(name)) and u.count:  # type: ignore[call-overload]
            lines.append(
                f"  {'':<14} not read by any template, not copied: {u.count} "
                f"({_size(u.size)}), e.g. {', '.join(u.sample[:3])}"
            )

    by_dataset: dict[tuple[str, str], list[Item]] = defaultdict(list)
    for i in items:
        by_dataset[(i.source.area, i.source.dataset_id)].append(i)
    ranked = sorted(
        by_dataset.items(),
        key=lambda kv: -sum(i.source.size for i in kv[1] if i.status == "copy"),
    )
    lines.append(f"Largest datasets to copy (top {top} of {len(ranked)}):")
    for (area, dataset_id), group in ranked[:top]:
        lines.append(f"  {area:<14} {dataset_id or '-':<40} {counts(group)}")

    lines.append("Sample mappings:")
    for name, group in by_area.items():
        a = areas[name]  # type: ignore[index]
        for i in [i for i in group if i.status == "copy"][:samples]:
            lines.append(
                f"  {a.source_bucket}/{i.source.key}  ->  {i.dest_bucket}/{i.dest_key}"
            )

    latest = [f for f in result.folders if f.kind == "latest" and f.status != "present"]
    if latest:
        lines.append(
            f"latest/ changes, first {min(top, len(latest))} of {len(latest)}:"
        )
        for f in latest[:top]:
            deletions = f", delete {len(f.deletions)}" if f.deletions else ""
            lines.append(
                f"  {f.area:<14} {f.dataset_id:<40} {f.status}: {f.reason}{deletions}"
            )

    version_conflicts = [
        f for f in result.folders if f.status == "conflict" and f.complete
    ]
    if version_conflicts:
        lines.append(
            f"Version conflicts (at both ends, different; never overwritten), "
            f"{len(version_conflicts)}, first {min(top, len(version_conflicts))}:"
        )
        for f in version_conflicts[:top]:
            lines.append(f"  {areas[f.area].dest_bucket}/{f.dest_prefix}")

    conflicts = [
        i
        for i in items
        if i.status == "conflict" and not (i.folder and i.folder.complete)
    ]
    if conflicts:
        lines.append(
            f"Conflicts (destination differs; skipped without --overwrite), "
            f"first {min(10, len(conflicts))} of {len(conflicts)}:"
        )
        lines += [f"  {i.dest_bucket}/{i.dest_key}" for i in conflicts[:10]]

    to_copy = sum(i.source.size for i in items if i.status == "copy")
    stale = sum(len(f.deletions) for f in result.folders)
    lines.append(
        f"Total to copy: {sum(i.status == 'copy' for i in items)} objects, {_size(to_copy)}"
        + (f"; {stale} stale latest/ objects to delete." if stale else ".")
    )
    if to_copy:
        estimates = ", ".join(
            f"{_duration(to_copy / (mb * 1024 * 1024))} at {mb} MB/s"
            for mb in (10, 25, 50, 100)
        )
        lines.append(f"Estimated time: {estimates}.")
    return "\n".join(lines)


def execute_summary(result: Plan) -> str:
    touched = [f for f in result.folders if any(i.status == "copy" for i in f.items)]
    done = sum(f.done for f in touched)
    line = f"Folders completed: {done} of {len(touched)}"
    if done < len(touched):
        line += " (the rest have no marker yet: rerun to finish them)"
    return line + "."


REFRESH_HINT = (
    "Next: materialize `version_catalog` in Dagster (group `catalog`) so the catalog, and "
    "then ingest_archive_sync, see what was copied."
)


# --- clients ---------------------------------------------------------------------------------


class MigrationConfigError(RuntimeError):
    pass


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise MigrationConfigError(f"{name} is not set.")
    return value


def clients(dest_endpoint: str | None, ca_bundle: str | None):
    """(source, destination) clients, built with dcpy.utils.s3.client so the endpoint
    resolver and DCPY_S3_ENDPOINT_DENYLIST apply. The source is DigitalOcean, so a denylist
    that refuses it is an error here, not something to work around."""
    source_endpoint = _required("AWS_S3_ENDPOINT")
    dest_endpoint = dest_endpoint or _required("AZ_S3_ENDPOINT")
    if s3.endpoint_host(source_endpoint) == s3.endpoint_host(dest_endpoint):
        raise MigrationConfigError(
            f"Source and destination are the same host: {source_endpoint}"
        )
    try:
        s3.resolve_endpoint(source_endpoint)
    except s3.S3ConfigError as e:
        raise MigrationConfigError(
            f"{e} This script reads DigitalOcean: unset {s3.ENDPOINT_DENYLIST_ENV_VAR} "
            "for this run (e.g. `env -u DCPY_S3_ENDPOINT_DENYLIST ...`)."
        ) from e
    source = s3.client(
        endpoint=source_endpoint,
        access_key_id=_required("AWS_ACCESS_KEY_ID"),
        secret_access_key=_required("AWS_SECRET_ACCESS_KEY"),
    )
    dest = s3.client(
        endpoint=dest_endpoint,
        access_key_id=_required("AZ_S3_ACCESS_KEY_ID"),
        secret_access_key=_required("AZ_S3_SECRET_ACCESS_KEY"),
        verify=ca_bundle or None,
    )
    return source, dest


# --- CLI -------------------------------------------------------------------------------------

app = typer.Typer(add_completion=False)


@app.command()
def migrate(
    area: list[str] = typer.Option(
        list(ALL_AREAS), "--area", "-a", help=f"Repeatable. One of {', '.join(ALL_AREAS)}."
    ),
    dataset: list[str] = typer.Option(
        [], "--dataset", "-d", help="Repeatable. Dataset id or glob (template id for inbox, product for publishing)."
    ),
    datasets_file: Path | None = typer.Option(
        None, help="File of dataset ids/globs, one per line; # comments allowed."
    ),
    latest_only: bool = typer.Option(
        False, help="Only each dataset's latest/ and the version it points at."
    ),
    versions_since: datetime | None = typer.Option(
        None, formats=["%Y%m%d"], help="YYYYMMDD. Only version folders written on or after it."
    ),
    execute_: bool = typer.Option(False, "--execute", help="Copy. Without it, a dry run."),
    overwrite: bool = typer.Option(
        False, help="Replace objects that differ at the destination. Never complete version folders, nor latest/."
    ),
    trust_catalog: bool = typer.Option(
        False, help="Skip the config.json HEAD for folders the version catalog lists: faster, but misses version conflicts and stale rows."
    ),
    verify: bool = typer.Option(False, help="Also check present objects' stored source ETag."),
    include_unmatched_inbox: bool = typer.Option(
        False, help="Inbox areas: also copy files no template reads (not with --dataset)."
    ),
    workers: int = typer.Option(8, help="Objects copied at once."),
    part_size_mb: int = typer.Option(16, help="Multipart part size and threshold, MB."),
    threads_per_file: int = typer.Option(4, help="Parallel parts per large object."),
    dest_endpoint: str | None = typer.Option(
        None, envvar="AZ_S3_ENDPOINT", show_envvar=True, help="Gateway URL."
    ),
    ca_bundle: str | None = typer.Option(
        None, envvar="AZ_S3_CA_BUNDLE", show_envvar=True, help="CA bundle for the gateway's certificate."
    ),
    recipes_root_folder: str = typer.Option(
        default_factory=recipes_root_folder_from_env, help="Destination folder for datasets/ and raw/ (env AZ_RECIPES_BUCKET_ROOT_FOLDER, RECIPES_BUCKET_ROOT_FOLDER)."
    ),
    inbox_root_folder: str = typer.Option(
        default_factory=inbox_root_folder_from_env, help="Destination folder for edm-private inbox files (env INBOX_ROOT_FOLDER)."
    ),
    templates_dir: Path = typer.Option(DEFAULT_TEMPLATES_DIR),
    source_recipes_bucket: str = typer.Option("edm-recipes", rich_help_panel="Buckets"),
    source_private_bucket: str = typer.Option("edm-private", rich_help_panel="Buckets"),
    source_publishing_bucket: str = typer.Option("edm-publishing", rich_help_panel="Buckets"),
    dest_recipes_bucket: str = typer.Option(
        "edm-recipes", envvar="AZ_RECIPES_BUCKET", rich_help_panel="Buckets"
    ),
    dest_publishing_bucket: str = typer.Option(
        "edm-publishing", envvar="AZ_PUBLISHING_BUCKET", rich_help_panel="Buckets"
    ),
):  # fmt: skip
    """Copy DigitalOcean Spaces into Azure Blob through the gateway. Dry run unless --execute."""
    unknown = set(area) - set(ALL_AREAS)
    if unknown:
        raise typer.BadParameter(f"Unknown area(s): {', '.join(sorted(unknown))}")
    patterns = list(dataset) + (
        read_datasets_file(datasets_file) if datasets_file else []
    )
    selection = Selection(
        areas=tuple(a for a in ALL_AREAS if a in area),  # type: ignore[misc]
        datasets=tuple(patterns),
        latest_only=latest_only,
        versions_since=versions_since.date() if versions_since else None,
        include_unmatched_inbox=include_unmatched_inbox,
    )
    buckets = Buckets(
        source_recipes_bucket,
        source_private_bucket,
        source_publishing_bucket,
        dest_recipes_bucket,
        dest_publishing_bucket,
    )
    areas = mapping(buckets, recipes_root_folder, inbox_root_folder)
    try:
        source, dest = clients(dest_endpoint, ca_bundle)
    except (MigrationConfigError, s3.S3ConfigError) as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(2)
    logger.info(
        f"Source {source.meta.endpoint_url} -> destination {dest.meta.endpoint_url}"
    )

    covered = {
        areas[a].dest_prefix for a in selection.areas if catalog_covers(areas[a])
    }
    catalog = load_catalog(dest_recipes_bucket, covered) if covered else None

    unmatched: dict[AreaName, Unmatched] = {}
    result = plan(
        source,
        dest,
        areas,
        selection,
        inbox_sources(templates_dir),
        overwrite=overwrite,
        verify=verify,
        workers=workers,
        unmatched=unmatched,
        catalog=catalog,
        trust_catalog=trust_catalog,
    )
    typer.echo(summary(result, areas, unmatched))
    if not execute_:
        typer.echo("Dry run: nothing copied. Add --execute to copy.")
        return
    failures = execute(
        source,
        dest,
        areas,
        result.items,
        workers=workers,
        part_size_mb=part_size_mb,
        threads_per_file=threads_per_file,
    )
    typer.echo(execute_summary(result))
    typer.echo(REFRESH_HINT)
    if failures:
        typer.echo(f"{len(failures)} objects failed; rerun to retry them.", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
