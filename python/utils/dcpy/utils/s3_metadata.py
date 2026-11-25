"""Custom S3 object metadata (x-amz-meta-*) that works on DigitalOcean Spaces and on Azure Blob
behind the versitygw gateway.

Azure metadata names must be C#-identifier-like, so the gateway rejects `date-created` with
400 InvalidMetadata, and Azure hands names back with their case changed (`commit` comes back
as `Commit`). So every writer goes through `for_write`, which lowercases names and turns
anything else into underscores, and every reader through `for_read`, which looks names up the
same way. Archives already on DigitalOcean carry the old hyphenated names (`date-created`,
`run-url`); they read the same as the new ones.
"""

import os
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import pytz

from dcpy.utils import git

DATE_CREATED = "date_created"
COMMIT = "commit"
RUN_URL = "run_url"


def normalize_key(key: str) -> str:
    """`Date-Created`, `date-created` and `date_created` all become `date_created`."""
    name = re.sub(r"[^0-9a-z_]", "_", key.strip().lower())
    return f"_{name}" if not name or name[0].isdigit() else name


class ObjectMetadata(dict[str, str]):
    """Metadata read from an object. Lookups ignore case and `-` vs `_`, so
    `md["date-created"]`, `md.get("Date_Created")` and `md[DATE_CREATED]` agree."""

    def __init__(self, metadata: Mapping[str, Any] | None = None):
        super().__init__(
            {normalize_key(k): str(v) for k, v in (metadata or {}).items()}
        )

    def __getitem__(self, key: str) -> str:
        return super().__getitem__(normalize_key(key))

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and super().__contains__(normalize_key(key))

    def get(self, key: str, default: Any = None) -> Any:  # type: ignore[override]
        return super().get(normalize_key(key), default)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Mapping):
            return dict.__eq__(self, ObjectMetadata(other))
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]


def for_read(metadata: Mapping[str, Any] | None) -> ObjectMetadata:
    return ObjectMetadata(metadata)


def for_write(metadata: Mapping[str, Any] | None) -> dict[str, str]:
    """Metadata in a form both DigitalOcean and Azure accept."""
    metadata = metadata or {}
    written: dict[str, str] = {}
    for key, value in metadata.items():
        name = normalize_key(key)
        if name in written and written[name] != str(value):
            raise ValueError(f"Metadata keys collide as '{name}': {list(metadata)}")
        written[name] = str(value)
    return written


def standard() -> dict[str, str]:
    """The metadata we stamp on uploads: creation time, commit, and the CI run if any."""
    metadata = {
        DATE_CREATED: datetime.now(pytz.timezone("America/New_York")).isoformat()
    }
    try:
        metadata[COMMIT] = git.commit_hash()
    except Exception:
        pass  # No git repo or commit available
    if os.environ.get("CI"):
        metadata[RUN_URL] = git.action_url()
    return metadata
