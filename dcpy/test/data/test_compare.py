import pandas as pd

from dcpy.data import compare
from dcpy.models.data import comparison


class TestDataFrame:
    basic = pd.DataFrame(
        {"b": [1, 1, 2], "l": [1, 2, 1], "c": ["test", "test2", "test"]}
    )
    different_column_values = pd.DataFrame(
        {"b": [1, 1, 2], "l": [1, 2, 1], "c": ["test", "test2", "test2"]}
    )
    different_columns = pd.DataFrame(
        {"b": [1, 1, 2], "l": [1, 2, 1], "d": ["test", "test2", "test2"]}
    )
    different_column_type = pd.DataFrame(
        {"b": [1, 1, 2], "l": [1, 2, 1], "c": [3, 4, 5]}
    )
    missing_key = pd.DataFrame({"b": [1, 1], "l": [1, 2], "c": ["test", "test2"]})
    different_keys = pd.DataFrame(
        {"b": [1, 1, 2], "l": [1, 2, 2], "c": ["test", "test2", "test"]}
    )

    def test_different_column_values(self):
        keys = ["b", "l"]
        key_difference = pd.DataFrame(
            {"b": [2], "l": [1], "left": ["test"], "right": ["test2"]}
        ).set_index(keys)
        expected = comparison.Report(
            row_count=comparison.Simple[int](left=3, right=3),
            column_comparison=comparison.Columns(
                both={"b", "l", "c"},
                left_only=set(),
                right_only=set(),
                type_differences={},
            ),
            data_comparison=comparison.KeyedTable(
                key_columns=keys,
                left_only=set(),
                right_only=set(),
                are_equal=False,
                columns_with_diffs={"c"},
                differences_by_column={"c": key_difference},
            ),
        )

        report = compare.get_df_keyed_report(
            self.basic, self.different_column_values, keys
        )
        assert report == expected

    def test_different_column_type(self):
        keys = ["b", "l"]
        key_difference = pd.DataFrame(
            {
                "b": [1, 1, 2],
                "l": [1, 2, 1],
                "left": ["test", "test2", "test"],
                "right": [3, 4, 5],
            }
        ).set_index(keys)
        expected = comparison.Report(
            row_count=comparison.Simple[int](left=3, right=3),
            column_comparison=comparison.Columns(
                both={"b", "l", "c"},
                left_only=set(),
                right_only=set(),
                type_differences={
                    "c": comparison.Simple[str](left="object", right="int64")
                },
            ),
            data_comparison=comparison.KeyedTable(
                key_columns=keys,
                left_only=set(),
                right_only=set(),
                are_equal=False,
                columns_with_diffs={"c"},
                differences_by_column={"c": key_difference},
            ),
        )

        report = compare.get_df_keyed_report(
            self.basic, self.different_column_type, keys
        )
        assert report == expected

    def test_different_columns(self):
        keys = ["b", "l"]
        expected = comparison.Report(
            row_count=comparison.Simple[int](left=3, right=3),
            column_comparison=comparison.Columns(
                both={"b", "l"},
                left_only={"c"},
                right_only={"d"},
                type_differences={},
            ),
            data_comparison=comparison.KeyedTable(
                key_columns=keys,
                left_only=set(),
                right_only=set(),
                are_equal=True,
                columns_with_diffs=set(),
                differences_by_column={},
            ),
        )

        report = compare.get_df_keyed_report(self.basic, self.different_columns, keys)
        assert report == expected

    def test_missing_key(self):
        keys = ["b", "l"]
        expected = comparison.Report(
            row_count=comparison.Simple[int](left=3, right=2),
            column_comparison=comparison.Columns(
                both={"b", "l", "c"},
                left_only=set(),
                right_only=set(),
                type_differences={},
            ),
            data_comparison=comparison.KeyedTable(
                key_columns=keys,
                left_only={(2, 1)},
                right_only=set(),
                are_equal=False,
                columns_with_diffs=set(),
                differences_by_column={},
            ),
        )

        report = compare.get_df_keyed_report(self.basic, self.missing_key, keys)
        assert report == expected

    def test_different_keys(self):
        keys = ["b", "l"]
        expected = comparison.Report(
            row_count=comparison.Simple[int](left=3, right=3),
            column_comparison=comparison.Columns(
                both={"b", "l", "c"},
                left_only=set(),
                right_only=set(),
                type_differences={},
            ),
            data_comparison=comparison.KeyedTable(
                key_columns=keys,
                left_only={(2, 1)},
                right_only={(2, 2)},
                are_equal=False,
                columns_with_diffs=set(),
                differences_by_column={},
            ),
        )

        report = compare.get_df_keyed_report(self.basic, self.different_keys, keys)
        assert report == expected
