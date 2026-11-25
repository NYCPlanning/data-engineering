"""Copy data from DigitalOcean Spaces into Azure Blob, through the versitygw S3 gateway, remapping
the layout on the way. See "Migrating data from DigitalOcean" in apps/docker/README.md.

Dry run by default: lists both ends and prints what would be copied. `--execute` copies.
Reruns skip objects already at the destination with the same size, so an interrupted run
continues where it stopped.

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
from collections import defaultdict
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
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


@dataclass
class Item:
    source: SourceObject
    dest_bucket: str
    dest_key: str
    status: Literal["copy", "present", "conflict", "unverified"] = "copy"


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


def _verify(dest, item: Item) -> None:
    head = dest.head_object(Bucket=item.dest_bucket, Key=item.dest_key)
    stored = s3_metadata.for_read(head.get("Metadata")).get(SOURCE_ETAG)
    if stored is None:
        item.status = "unverified"
    elif stored != item.source.etag:
        item.status = "conflict"


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
) -> list[Item]:
    """What to copy: every selected source object, with what the destination already has.

    Same size at the destination is `present` (skipped). A different size is a `conflict`,
    left alone unless `overwrite`. With `verify`, present objects are also checked against
    the source ETag stamped on them at copy time.
    """
    items: list[Item] = []
    for name in selection.areas:
        area = areas[name]
        found = None
        if unmatched is not None and area.kind == "inbox":
            found = unmatched.setdefault(name, Unmatched())
        objects = list(list_area(source, area, selection, sources, found))
        objects = filter_versions(source, area, objects, selection, workers)
        logger.info(f"{name}: {len(objects)} objects selected in {area.source_bucket}")
        if not objects:
            continue
        mapped = [dest_key(area, o.key) for o in objects]
        # One listing of the destination per dataset folder keeps it close to the selection.
        by_id: dict[str, list[str]] = defaultdict(list)
        for o, key in zip(objects, mapped):
            by_id[o.dataset_id].append(key)
        index = _dest_index(
            dest,
            area.dest_bucket,
            (_common_prefix(keys) for keys in by_id.values()),
            workers,
        )
        for o, key in zip(objects, mapped):
            item = Item(o, area.dest_bucket, key)
            if key in index:
                item.status = "present" if index[key] == o.size else "conflict"
                if item.status == "conflict" and overwrite:
                    item.status = "copy"
            items.append(item)

    if verify:
        present = [i for i in items if i.status == "present"]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for future in as_completed(pool.submit(_verify, dest, i) for i in present):
                future.result()
        if overwrite:
            for i in items:
                if i.status == "conflict":
                    i.status = "copy"
    return items


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
    todo = [i for i in items if i.status == "copy"]
    # Big files first: they set the run's length, and small ones fill in around them.
    todo.sort(key=lambda i: -i.source.size)
    config = transfer_config(part_size_mb, threads_per_file)
    progress = _Progress(len(todo), sum(i.source.size for i in todo), log_every_seconds)
    failures: list[tuple[Item, Exception]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(copy_one, source, dest, areas, i, config): i for i in todo
        }
        for future in as_completed(futures):
            item = futures[future]
            try:
                future.result()
                progress.add(item.source.size)
            except Exception as e:
                logger.error(f"Failed: {item.source.key} -> {item.dest_key}: {e}")
                failures.append((item, e))
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


def summary(
    items: list[Item],
    areas: dict[AreaName, Area],
    unmatched: dict[AreaName, Unmatched] | None = None,
    top: int = 25,
    samples: int = 3,
) -> str:
    lines = []
    statuses = ("copy", "present", "conflict", "unverified")
    by_area: dict[str, list[Item]] = defaultdict(list)
    for i in items:
        by_area[i.source.area].append(i)

    def counts(group: list[Item]) -> str:
        cells = []
        for status in statuses:
            matching = [i for i in group if i.status == status]
            if matching or status in ("copy", "present"):
                size = sum(i.source.size for i in matching)
                cells.append(f"{status} {len(matching)} ({_size(size)})")
        return ", ".join(cells)

    lines.append("By area:")
    for name, group in by_area.items():
        a = areas[name]  # type: ignore[index]
        lines.append(
            f"  {name:<14} {a.source_bucket}/{a.source_prefix} -> "
            f"{a.dest_bucket}/{a.dest_prefix}: {counts(group)}"
        )
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

    conflicts = [i for i in items if i.status == "conflict"]
    if conflicts:
        lines.append(
            f"Conflicts (destination differs; skipped without --overwrite), "
            f"first {min(10, len(conflicts))}:"
        )
        lines += [f"  {i.dest_bucket}/{i.dest_key}" for i in conflicts[:10]]

    to_copy = sum(i.source.size for i in items if i.status == "copy")
    lines.append(
        f"Total to copy: {sum(i.status == 'copy' for i in items)} objects, {_size(to_copy)}."
    )
    if to_copy:
        estimates = ", ".join(
            f"{_duration(to_copy / (mb * 1024 * 1024))} at {mb} MB/s"
            for mb in (10, 25, 50, 100)
        )
        lines.append(f"Estimated time: {estimates}.")
    return "\n".join(lines)


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
    overwrite: bool = typer.Option(False, help="Replace objects that differ at the destination."),
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

    unmatched: dict[AreaName, Unmatched] = {}
    items = plan(
        source,
        dest,
        areas,
        selection,
        inbox_sources(templates_dir),
        overwrite=overwrite,
        verify=verify,
        workers=workers,
        unmatched=unmatched,
    )
    typer.echo(summary(items, areas, unmatched))
    if not execute_:
        typer.echo("Dry run: nothing copied. Add --execute to copy.")
        return
    failures = execute(
        source,
        dest,
        areas,
        items,
        workers=workers,
        part_size_mb=part_size_mb,
        threads_per_file=threads_per_file,
    )
    if failures:
        typer.echo(f"{len(failures)} objects failed; rerun to retry them.", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
