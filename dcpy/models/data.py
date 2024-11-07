from __future__ import annotations
import pandas as pd
from pydantic import BaseModel, Field
from typing import TypeVar, Generic


T = TypeVar("T")


class Comparison:
    class Simple(BaseModel, Generic[T]):
        left: T
        right: T

    class Columns(BaseModel):
        both: set[str]
        left_only: set[str]
        right_only: set[str]
        type_differences: dict[str, Comparison.Simple[str]]

    class KeyedTable(BaseModel, arbitrary_types_allowed=True):
        key_columns: list[str]
        left_only: set = Field(serialization_alias="Keys found in left only")
        right_only: set = Field(serialization_alias="Keys found in right only")
        columns_with_diffs: set[str] = Field(
            serialization_alias="Columns with changed values for specific keys"
        )
        differences_by_column: dict[str, pd.DataFrame] = Field(
            serialization_alias="Changed values by column"
        )

    class SimpleTable(BaseModel, arbitrary_types_allowed=True):
        compared_columns: set[str]
        left_only: pd.DataFrame | None
        right_only: pd.DataFrame | None

    class Report(BaseModel, arbitrary_types_allowed=True):
        row_count: Comparison.Simple[int]
        column_comparison: Comparison.Columns
        data_comparison: Comparison.KeyedTable | Comparison.SimpleTable
