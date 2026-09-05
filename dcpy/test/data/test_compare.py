import geopandas as gpd
import pandas as pd
import shapely

from dcpy.data import compare
from dcpy.data import models as comparison


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
                    # Derive expected dtypes from the inputs so this is
                    # version-agnostic (pandas 3.0 renamed object -> str).
                    "c": comparison.Simple[str](
                        left=str(self.basic["c"].dtype),
                        right=str(self.different_column_type["c"].dtype),
                    )
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


class TestGeometryTolerance:
    """Coordinates should absorb float noise but nothing structural."""

    # State Plane-ish magnitudes, where GEOMETRY_RTOL works out to ~1e-6 feet
    square = shapely.Polygon([(1e6, 2e5), (1e6 + 10, 2e5), (1e6 + 10, 2e5 + 10)])
    nudged = shapely.Polygon([(1e6 + 1e-7, 2e5), (1e6 + 10, 2e5), (1e6 + 10, 2e5 + 10)])
    moved = shapely.Polygon([(1e6 + 1, 2e5), (1e6 + 10, 2e5), (1e6 + 10, 2e5 + 10)])
    extra_vertex = shapely.Polygon(
        [(1e6, 2e5), (1e6 + 5, 2e5), (1e6 + 10, 2e5), (1e6 + 10, 2e5 + 10)]
    )

    def _series(self, *geoms):
        return gpd.GeoSeries(list(geoms))

    def test_identical(self):
        assert compare.geometries_match(
            self._series(self.square), self._series(self.square)
        )

    def test_float_noise_tolerated(self):
        assert compare.geometries_match(
            self._series(self.square), self._series(self.nudged)
        )

    def test_real_move_caught(self):
        assert not compare.geometries_match(
            self._series(self.square), self._series(self.moved)
        )

    def test_added_vertex_caught(self):
        assert not compare.geometries_match(
            self._series(self.square), self._series(self.extra_vertex)
        )

    def test_null_placement_caught(self):
        assert not compare.geometries_match(
            self._series(self.square, None), self._series(None, self.square)
        )

    def test_geometry_type_caught(self):
        assert not compare.geometries_match(
            self._series(self.square), self._series(self.square.centroid)
        )

    def test_row_order_caught(self):
        assert not compare.geometries_match(
            self._series(self.square, self.moved), self._series(self.moved, self.square)
        )

    def test_two_dimensional_still_matches(self):
        """z is padded with NaN, which must not read as a difference."""
        assert compare.geometries_match(
            self._series(shapely.Point(1e6, 2e5)), self._series(shapely.Point(1e6, 2e5))
        )

    def test_z_change_caught(self):
        assert not compare.geometries_match(
            self._series(shapely.Point(1e6, 2e5, 10)),
            self._series(shapely.Point(1e6, 2e5, 9999)),
        )

    def test_flattening_to_two_dimensions_caught(self):
        assert not compare.geometries_match(
            self._series(shapely.Point(1e6, 2e5, 10)),
            self._series(shapely.Point(1e6, 2e5)),
        )

    def test_z_float_noise_tolerated(self):
        assert compare.geometries_match(
            self._series(shapely.Point(1e6, 2e5, 1e5)),
            self._series(shapely.Point(1e6, 2e5, 1e5 + 1e-8)),
        )


class TestDataFramesMatch:
    square = TestGeometryTolerance.square
    nudged = TestGeometryTolerance.nudged
    moved = TestGeometryTolerance.moved

    def _frame(self, geom, name="a"):
        return gpd.GeoDataFrame({"name": [name], "geom": [geom]}, geometry="geom")

    def test_float_noise_tolerated(self):
        assert compare.dataframes_match(
            self._frame(self.square), self._frame(self.nudged)
        )

    def test_real_move_caught(self):
        assert not compare.dataframes_match(
            self._frame(self.square), self._frame(self.moved)
        )

    def test_attribute_change_caught(self):
        """Tolerance applies to geometry only - other columns stay exact."""
        assert not compare.dataframes_match(
            self._frame(self.square), self._frame(self.nudged, name="b")
        )

    def test_geometry_column_against_plain_column(self):
        """Matching column names don't imply matching types."""
        plain = pd.DataFrame({"name": ["a"], "geom": ["POINT (1 2)"]})
        assert not compare.dataframes_match(self._frame(self.square), plain)

    def test_non_geospatial_falls_back_to_exact(self):
        left = pd.DataFrame({"a": [1.0]})
        assert compare.dataframes_match(left, pd.DataFrame({"a": [1.0]}))
        assert not compare.dataframes_match(left, pd.DataFrame({"a": [1.0 + 1e-15]}))
