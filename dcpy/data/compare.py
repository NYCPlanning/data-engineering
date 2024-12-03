import pandas as pd

from dcpy.models.data import comparison
from dcpy.utils import postgres


def compare_df_columns(left: pd.DataFrame, right: pd.DataFrame):
    lc_set = set(left.columns)
    rc_set = set(right.columns)

    type_differences = {}

    for column in lc_set & rc_set:
        left_dtype = str(left[column].dtype)
        right_dtype = str(right[column].dtype)

        if left_dtype != right_dtype:
            type_differences[column] = comparison.Simple[str](
                left=left_dtype, right=right_dtype
            )

    return comparison.Columns(
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

    return comparison.KeyedTable(
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
        left_dtype = left_types[column]
        right_dtype = right_types[column]

        if left_dtype != right_dtype:
            type_differences[column] = comparison.Simple[str](
                left=left_dtype, right=right_dtype
            )

    return comparison.Columns(
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

    def query(column: str) -> str:
        lc = f'"left"."{column}"'
        rc = f'"right"."{column}"'
        return f"""
            SELECT 
                {left_keys}, {lc} AS "left", {rc} AS "right" 
            FROM {left} AS "left" 
                INNER JOIN {right} AS "right"
                ON {on}
            WHERE {lc} IS DISTINCT FROM {rc}
        """

    def spatial_query(column: str) -> str:
        lc = f'"left"."{column}"'
        rc = f'"right"."{column}"'
        return f"""
            SELECT 
                {left_keys},
                st_orderingequals({lc}, {rc}) AS "ordering_equal",
                st_equals({lc}, {rc}) AS "spatially_equal",
                st_geometrytype({lc}) AS "left_geom_type",
                st_geometrytype({rc}) AS "right_geom_type"
            FROM {left} AS "left" 
                INNER JOIN {right} AS "right"
                ON {on}
            WHERE {lc} IS DISTINCT FROM {rc}
        """

    left_geom_columns = client.get_geometry_columns(left)
    right_geom_columns = client.get_geometry_columns(right)

    for column in non_key_columns:
        # simple inequality is not informative for spatial columns
        if (column in left_geom_columns) and (column in right_geom_columns):
            comp_df = client.execute_select_query(spatial_query(column))
            comp_df = comp_df.set_index(key_columns)
            comp_df.columns = pd.Index(
                [
                    "ordering_equal",
                    "spatially_equal",
                    "left_geom_type",
                    "right_geom_type",
                ]
            )

        elif (column not in left_geom_columns) and (column not in right_geom_columns):
            comp_df = client.execute_select_query(query(column))
            comp_df = comp_df.set_index(key_columns)
            comp_df.columns = pd.Index(["left", "right"])

        # No point comparing geom and non-geom.
        # This should be caught in `column_comparison` of report
        # Other non-equivalent types are allowed - text vs varchar can produce valid comps
        else:
            continue

        if len(comp_df) > 0:
            comps[column] = comp_df.copy()

    return comparison.KeyedTable(
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
    query_columns = ",".join(f'"{c}"' for c in columns)

    def query(one, two):
        return client.execute_select_query(f"""
            SELECT {query_columns} FROM {one}
            EXCEPT
            SELECT {query_columns} FROM {two}
        """)

    return comparison.SimpleTable(
        compared_columns=columns,
        left_only=query(left, right),
        right_only=query(right, left),
    )


def get_df_keyed_report(
    left: pd.DataFrame, right: pd.DataFrame, key_columns: list[str]
):
    return comparison.Report(
        row_count=comparison.Simple[int](left=len(left), right=len(right)),
        column_comparison=compare_df_columns(left, right),
        data_comparison=compare_df_keyed_rows(left, right, key_columns),
    )


def get_sql_report(
    left: str,
    right: str,
    client: postgres.PostgresClient,
    *,
    key_columns: list[str] | None = None,
    ignore_columns: list[str] | None = None,
    columns_only_comparison: bool = False,
) -> comparison.SqlReport:
    left_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {left}")
    right_rows = client.execute_select_query(f"SELECT count(*) AS count FROM {right}")
    if columns_only_comparison:
        data_comp: comparison.KeyedTable | comparison.SimpleTable | None = None
    elif key_columns:
        data_comp = compare_sql_keyed_rows(
            left,
            right,
            key_columns,
            client,
            ignore_columns=ignore_columns,
        )
    else:
        data_comp = compare_sql_rows(
            left,
            right,
            client,
            ignore_columns=ignore_columns,
        )
    return comparison.SqlReport(
        tables=comparison.Simple[str](left=left, right=right),
        row_count=comparison.Simple[int](
            left=left_rows["count"][0], right=right_rows["count"][0]
        ),
        column_comparison=compare_sql_columns(left, right, client),
        data_comparison=data_comp,
    )
