from typing import Any, Callable, Literal

from pydantic import field_validator

from dcpy.models.base import SortedSerializedBase

COLUMN_TYPES = Literal[
    "bbl",
    "bool",
    "date",
    "datetime",
    "decimal",
    "geometry",
    "integer",
    "number",  # TODO: Need to delete. Keeping it now for compatibility with metadata files
    "text",
]


# TODO: DELETE
class Checks(SortedSerializedBase):
    is_primary_key: bool | None = None
    non_nullable: bool | None = None


class CheckAttributes(SortedSerializedBase, extra="forbid"):
    """
    Defines the settings and parameters for a column data check,
    aligning with the `pandera.Check` object.

    This class mirrors the `pandera.Check` constructor, where the `args` property
    holds parameters specific to individual checks (e.g., thresholds or conditions).
    Additional fields in this class configure options such as whether to raise
    warnings or how to handle missing data.
    """

    args: dict[str, Any]
    description: str | None = None
    warn_only: bool = False
    name: str | None = None
    title: str | None = None
    n_failure_cases: int | None = None
    groups: str | list[str] | None = None
    groupby: str | list[str] | Callable | None = None
    ignore_na: bool = True


class Column(SortedSerializedBase, extra="forbid"):
    """
    An extensible base class for defining column metadata in ingest and product templates.
    """

    id: str
    data_type: COLUMN_TYPES | None = None
    description: str | None = None
    is_required: bool = True
    checks: Checks | list[str | dict[str, CheckAttributes]] | None = (
        None  # TODO: delete Checks after refactoring metadata
    )

    @field_validator("checks", mode="after")
    @classmethod
    def check_checks(cls, checks: list[str | dict[str, CheckAttributes]]):
        if isinstance(checks, list):
            for check in checks:
                if isinstance(check, dict) and len(check) != 1:
                    raise ValueError(f"{check} must contain exactly one key-value pair")
        return checks
