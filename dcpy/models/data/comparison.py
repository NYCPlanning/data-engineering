import pandas as pd
from pydantic import BaseModel, Field
from typing import TypeVar, Generic

from dcpy.models.base import SortedSerializedBase, ModelWithDataFrame

T = TypeVar("T")


class Simple(BaseModel, Generic[T]):
    left: T
    right: T


class Columns(BaseModel):
    both: set[str]
    left_only: set[str]
    right_only: set[str]
    type_differences: dict[str, Simple[str]]


class KeyedTable(ModelWithDataFrame):
    key_columns: list[str]
    left_only: set = Field(serialization_alias="Keys found in left only")
    right_only: set = Field(serialization_alias="Keys found in right only")
    are_equal: bool
    ignored_columns: list[str] | None = None
    columns_coerced_to_numeric: list[str] | None = None
    columns_with_diffs: set[str] = Field(
        serialization_alias="Columns with changed values for specific keys"
    )
    differences_by_column: dict[str, pd.DataFrame] = Field(
        serialization_alias="Changed values by column"
    )


class SimpleTable(ModelWithDataFrame):
    compared_columns: set[str]
    ignored_columns: list[str] | None = None
    columns_coerced_to_numeric: list[str] | None = None
    left_only: pd.DataFrame | None
    right_only: pd.DataFrame | None
    are_equal: bool


class Report(SortedSerializedBase):
    row_count: Simple[int]
    column_comparison: Columns
    data_comparison: KeyedTable | SimpleTable | None

    _exclude_falsey_values: bool = False
    _head_sort_order: list[str] = ["row_count"]
    _tail_sort_order: list[str] = ["column_comparison", "data_comparison"]


class SqlReport(Report):
    tables: Simple[str]

    _head_sort_order: list[str] = ["tables", "row_count"]
