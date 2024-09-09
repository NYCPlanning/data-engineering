import pandas as pd
from pydantic import BaseModel
from typing import TypeVar, Generic

from dcpy.models.base import IndentedPrint
from dcpy.utils import postgres


T = TypeVar("T")


class SimpleComparison(IndentedPrint, Generic[T]):
    left: T
    right: T


class EntryWiseComparison(BaseModel, Generic[T]):
    left_only: T
    right_only: T
    both: T


class ColumnsComparison(IndentedPrint):
    both: set[str]
    left_only: set[str]
    right_only: set[str]
    type_differences: dict[str, SimpleComparison[str]]

    def to_list_structure(self):
        return [
            "Column Comparison",
            "Columns in both:",
            self.both,
            "Column found in left only:",
            self.left_only,
            "Column found in right only:",
            self.right_only,
            "Columns in both with different types:",
            *[
                [column, self.type_differences[column]]
                for column in self.type_differences
            ],
        ]


class KeyedTableComparison(IndentedPrint, arbitrary_types_allowed=True):
    _title = "Keyed Table Comparison"
    _display_names = {
        "left_only": "Keys found in left only",
        "right_only": "Keys found in right only",
        "columns_with_diffs": "Columns with changed values for specific keys",
        "differences_by_column": "Changed values by column",
    }
    key_columns: list[str]
    left_only: set
    right_only: set
    columns_with_diffs: set[str]
    differences_by_column: dict[str, pd.DataFrame]


class SimpleTableComparison(IndentedPrint, arbitrary_types_allowed=True):
    compared_columns: set[str]
    left_only: pd.DataFrame | None
    right_only: pd.DataFrame | None


class ComparisonReport(IndentedPrint, arbitrary_types_allowed=True):
    _include_line_breaks = True

    row_count: SimpleComparison[int]
    column_comparison: ColumnsComparison
    data_comparison: KeyedTableComparison | SimpleTableComparison


def compare_df_columns(left: pd.DataFrame, right: pd.DataFrame):
    lc_set = set(left.columns)
    rc_set = set(right.columns)

    def get_dtype(column: str, df: pd.DataFrame) -> str:
        if column in df.columns:
            return str(df[column].dtype)
        else:
            return "None"

    type_differences = {}

    for column in lc_set & rc_set:
        left_dtype = get_dtype(column, left)
        right_dtype = get_dtype(column, right)

        if left_dtype != right_dtype:
            type_differences[column] = SimpleComparison[str](
                left=left_dtype, right=right_dtype
            )

    return ColumnsComparison(
        both=lc_set & rc_set,
        left_only=lc_set - rc_set,
        right_only=rc_set - lc_set,
        type_differences=type_differences,
    )


def compare_df_keyed_rows(
    left: pd.DataFrame, right: pd.DataFrame, key_columns: list[str]
):
    assert set(key_columns).issubset(set(left.columns))
    assert set(key_columns).issubset(set(right.columns))
    left = left.set_index(key_columns)
    right = right.set_index(key_columns)

    columns = set(left.columns) & set(right.columns)
    left = left[list(columns)]
    right = right[list(columns)]

    merged: pd.DataFrame = pd.merge(
        left,
        right,
        left_on=key_columns,
        right_on=key_columns,
        how="outer",
        suffixes=("_left", "_right"),
        indicator=True,
    )
    left_only = set(merged[merged["_merge"] == "left_only"].index)
    right_only = set(merged[merged["_merge"] == "right_only"].index)

    comps: dict[str, pd.DataFrame] = {}

    both = merged[merged["_merge"] == "both"]
    for column in columns:
        comp_df = both[[column + "_left", column + "_right"]]
        comp_df = comp_df[comp_df[column + "_left"] != comp_df[column + "_right"]]
        comp_df.columns = pd.Index(["left", "right"])
        if len(comp_df) > 0:
            comps[column] = comp_df.copy()

    return KeyedTableComparison(
        key_columns=key_columns,
        left_only=left_only,
        right_only=right_only,
        columns_with_diffs=set(comps.keys()),
        differences_by_column=comps,
    )


def _df_to_set_of_lists(df: pd.DataFrame) -> set[list]:
    return set(list(df.itertuples(index=False, name=None)))  # type: ignore


def compare_sql_columns(left: str, right: str, client: postgres.PostgresClient):
    left_columns = set(client.get_table_columns(left))
    right_columns = set(client.get_table_columns(right))

    type_differences = {}

    left_types = client.get_column_types(left)
    right_types = client.get_column_types(right)

    for column in left_columns & right_columns:
        left_dtype = left_types.get(column, "None")
        right_dtype = right_types.get(column, "None")

        if left_dtype != right_dtype:
            type_differences[column] = SimpleComparison[str](
                left=left_dtype, right=right_dtype
            )

    return ColumnsComparison(
        both=left_columns & right_columns,
        left_only=left_columns - right_columns,
        right_only=right_columns - left_columns,
        type_differences=type_differences,
    )


def compare_sql_keyed_rows(
    left: str,
    right: str,
    key_columns: list[str],
    client: postgres.PostgresClient,
    *,
    ignore_columns: list[str] | None = None,
):
    left_columns = client.get_table_columns(left)
    right_columns = client.get_table_columns(right)
    assert set(key_columns).issubset(set(left_columns))
    assert set(key_columns).issubset(set(right_columns))

    columns = set(left_columns) & set(right_columns) - set(ignore_columns or [])
    non_key_columns = columns - set(key_columns)
    left_keys = ", ".join([f'"left"."{c}"' for c in key_columns])
    left_keys_alias = ", ".join([f'"left"."{c}" AS "{c}_left"' for c in key_columns])
    right_keys_alias = ", ".join([f'"right"."{c}"AS "{c}_right"' for c in key_columns])

    on = " AND ".join([f'"left"."{c}" = "right"."{c}"' for c in key_columns])
    from_clause = f'FROM {left} AS "left" FULL OUTER JOIN {right} AS "right" ON {on}'

    left_only = client.execute_select_query(
        f'SELECT {left_keys_alias} {from_clause} WHERE "right"."{key_columns[0]}" IS NULL'
    )
    right_only = client.execute_select_query(
        f'SELECT {right_keys_alias} {from_clause} WHERE "left"."{key_columns[0]}" IS NULL'
    )

    comps: dict[str, pd.DataFrame] = {}

    def query(column):
        lc = f'"left"."{column}"'
        rc = f'"right"."{column}"'
        return f"""
            SELECT 
                {left_keys}, {lc} AS "left", {rc} AS "right" 
            FROM {left} AS "left" 
                INNER JOIN {right} AS "right"
                ON {on}
            WHERE NOT ({lc} = {rc})
                OR ({lc} IS NULL and {rc} IS NOT NULL)
                OR ({rc} IS NULL and {lc} IS NOT NULL)
        """

    for column in non_key_columns:
        comp_df = client.execute_select_query(query(column))
        comp_df = comp_df.set_index(key_columns)
        comp_df.columns = pd.Index(["left", "right"])
        if len(comp_df) > 0:
            comps[column] = comp_df.copy()

    return KeyedTableComparison(
        key_columns=key_columns,
        left_only=_df_to_set_of_lists(left_only),
        right_only=_df_to_set_of_lists(right_only),
        columns_with_diffs=set(comps.keys()),
        differences_by_column=comps,
    )


def compare_sql_rows(
    left: str,
    right: str,
    client: postgres.PostgresClient,
    *,
    ignore_columns: list[str] | None = None,
):
    left_columns = client.get_table_columns(left)
    right_columns = client.get_table_columns(right)

    columns = set(left_columns) & set(right_columns) - set(ignore_columns or [])
    query_columns = ",".join(list(columns))

    def query(one, two):
        return client.execute_select_query(f"""
            SELECT {query_columns} FROM {one}
            EXCEPT
            SELECT {query_columns} FROM {two}
        """)

    return SimpleTableComparison(
        compared_columns=columns,
        left_only=query(left, right),
        right_only=query(right, left),
    )


def get_df_keyed_report(
    left: pd.DataFrame, right: pd.DataFrame, key_columns: list[str]
):
    return ComparisonReport(
        row_count=SimpleComparison[int](left=len(left), right=len(right)),
        column_comparison=compare_df_columns(left, right),
        data_comparison=compare_df_keyed_rows(left, right, key_columns),
    )


def get_sql_keyed_report(
    left: str,
    right: str,
    key_columns: list[str],
    client: postgres.PostgresClient,
    *,
    ignore_columns: list[str] | None = None,
) -> ComparisonReport:
    left_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {left}")
    right_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {right}")
    return ComparisonReport(
        row_count=SimpleComparison[int](
            left=left_rows["count"][0], right=right_rows["count"][0]
        ),
        column_comparison=compare_sql_columns(left, right, client),
        data_comparison=compare_sql_keyed_rows(
            left,
            right,
            key_columns,
            client,
            ignore_columns=ignore_columns,
        ),
    )


def get_sql_report(
    left: str,
    right: str,
    client: postgres.PostgresClient,
    *,
    ignore_columns: list[str] | None = None,
) -> ComparisonReport:
    left_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {left}")
    right_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {right}")
    return ComparisonReport(
        row_count=SimpleComparison[int](
            left=left_rows["count"][0], right=right_rows["count"][0]
        ),
        column_comparison=compare_sql_columns(left, right, client),
        data_comparison=compare_sql_rows(
            left,
            right,
            client,
            ignore_columns=ignore_columns,
        ),
    )
