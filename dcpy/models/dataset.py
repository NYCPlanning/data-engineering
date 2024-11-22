from dcpy.models.base import SortedSerializedBase
from dcpy.models.product.dataset.metadata_v2 import (
    Checks,
)  # TODO: implement Checks in this file
from typing import Literal

COLUMN_TYPES = Literal[
    "text",
    "integer",
    "decimal",
    "number",  # TODO: Need to delete. Keeping it now for compatibility with metadata files
    "geometry",
    "bool",
    "bbl",
    "date",
    "datetime",
]

# TODO: implement checks
# class Checks(SortedSerializedBase):


class Column(SortedSerializedBase, extra="forbid"):
    """
    An extensible base class for defining column metadata in ingest and product templates.
    """

    id: str
    data_type: COLUMN_TYPES | None = None
    description: str | None = None
    is_required: bool = True
    checks: Checks | None = None
