"""An optional index of archived input versions that planning can read instead of the bucket.

Planning resolves, for each recipes input, its latest version, file type, name, archive date and
source url: five or so bucket calls per input. A caller with an index of what's archived passes
it to `plan_recipe` (or `plan`) and planning asks it for every input in one batch. Any input it
returns None for is resolved from the bucket as before, so an index that's stale or partial can
slow planning down but not change what it plans. Without one, planning reads the bucket only.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from dcpy.connectors.edm.models import DatasetType


@dataclass(frozen=True)
class InputRequest:
    """One input to resolve. `version` may be "latest"."""

    key: str
    version: str


@dataclass(frozen=True)
class ResolvedInput:
    """What planning would otherwise read from the bucket for one input."""

    version: str
    file_type: DatasetType
    name: str | None
    archive_date: date | None
    url: str | None


class VersionIndex(Protocol):
    def resolve(
        self,
        source: str,
        inputs: Sequence[InputRequest],
        file_type_preference: Sequence[DatasetType],
    ) -> list[ResolvedInput | None]:
        """One result per input, in order: None where the index can't answer exactly
        (unknown input, or none of the preferred file types), and planning reads the bucket.

        `source` is the registry name of the connector the inputs come from.
        """
        ...
