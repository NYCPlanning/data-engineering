from datetime import date, datetime
import geopandas as gpd
import numpy as np
import pandas as pd
from pathlib import Path
from pydantic import TypeAdapter, BaseModel
import pytest
from shapely import Polygon, MultiPolygon, Point
import yaml
from unittest import TestCase, mock

from dcpy.models.file import Format
from dcpy.models.lifecycle.ingest import ProcessingStep, Column

from dcpy.utils import data
from dcpy.utils.geospatial import parquet as geoparquet
from dcpy.lifecycle.ingest import transform

from .shared import RESOURCES, TEST_DATA_DIR, TEST_DATASET_NAME, TEST_OUTPUT


class FakeConfig(BaseModel):
    """
    Test pydentic class used to validate input yaml file
    """

    file_name: str
    file_format: Format


def get_fake_data_configs():
    """
    Returns a list of dicts that represent data files in resources/test_data directory.
    Each dict contains a file.Format object and a path to the test data.
    """
    with open(RESOURCES / "transform_to_parquet_template.yml") as f:
        configs = TypeAdapter(list[FakeConfig]).validate_python(yaml.safe_load(f))

    test_files = []

    for config in configs:
        format = config.file_format
        file_name = config.file_name
        local_file_path = RESOURCES / TEST_DATA_DIR / file_name

        test_files.append({"format": format, "local_file_path": local_file_path})

    return test_files


@pytest.mark.parametrize("file", get_fake_data_configs())
def test_to_parquet(file: dict, create_temp_filesystem: Path):
    """
    Test the to_parquet function.

    Checks:
        - Checks if the function creates expected parquet file.
        - Checks if the saved Parquet file contains the expected data.
    """

    test_output_filename = "test_output.parquet"
    output_file_path = create_temp_filesystem / test_output_filename

    transform.to_parquet(
        file_format=file["format"],
        local_data_path=file["local_file_path"],
        dir=create_temp_filesystem,
        output_filename=test_output_filename,
    )

    assert output_file_path.is_file()

    output_df = pd.read_parquet(output_file_path)
    raw_df = data.read_data_to_df(
        data_format=file["format"], local_data_path=file["local_file_path"]
    )

    # rename geom column & translate geometry to wkb format to match parquet geom column
    if isinstance(raw_df, gpd.GeoDataFrame):
        raw_df = raw_df.rename_geometry(transform.OUTPUT_GEOM_COLUMN)
        raw_df = raw_df.to_wkb()

    print(f"raw_df:\n{raw_df.head()}")
    print(f"\noutput_df:\n{output_df.head()}")

    pd.testing.assert_frame_equal(
        left=raw_df,
        right=output_df,
        check_dtype=False,
        check_like=True,  # ignores order of rows and columns
    )


def test_validate_processing_steps():
    steps = [
        ProcessingStep(name="multi"),
        ProcessingStep(name="drop_columns", args={"columns": ["col1", "col2"]}),
    ]
    compiled_steps = transform.validate_processing_steps("test", steps)
    assert len(compiled_steps) == 2

    df = gpd.GeoDataFrame(
        {
            "col1": [1, 2, 3],
            "col2": [4, 5, 6],
            "col3": gpd.GeoSeries([None, None, None]),
        }
    ).set_geometry("col3")
    for step in compiled_steps:
        df = step(df)
    expected = gpd.GeoDataFrame(
        {"col3": gpd.GeoSeries([None, None, None])}
    ).set_geometry("col3")
    assert df.equals(expected)


@pytest.mark.parametrize(
    "step",
    [
        # Non-existent function
        ProcessingStep(name="fake_function_name"),
        # Missing arg
        ProcessingStep(name="drop_columns", args={}),
        # Unexpected arg
        ProcessingStep(name="drop_columns", args={"columns": [0], "fake_arg": 0}),
        # Invalid pd series func
        ProcessingStep(
            name="pd_series_func",
            args={"function_name": "str.fake_function", "column_name": "_"},
        ),
    ],
)
def test_validate_processing_steps_errors(step):
    with pytest.raises(Exception, match="Invalid processing steps"):
        transform.validate_processing_steps("test", [step])


class TestValidatePdSeriesFunc(TestCase):
    """transorm.validate_pd_series_func returns dictionary of validation errors"""

    def test_first_level(self):
        assert not transform.validate_pd_series_func(
            function_name="map", arg={"value 1": "other value 1"}
        )

    def test_str_series(self):
        assert not transform.validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl"
        )

    def test_missing_arg(self):
        assert "repl" in transform.validate_pd_series_func(
            function_name="str.replace", pat="pat"
        )

    def test_extra_arg(self):
        assert "extra_arg" in transform.validate_pd_series_func(
            function_name="str.replace", pat="pat", repl="repl", extra_arg="foo"
        )

    def test_invalid_function(self):
        res = transform.validate_pd_series_func(function_name="str.fake_function")
        assert res == "'pd.Series.str' has no attribute 'fake_function'"

    def test_gpd_without_flag(self):
        res = transform.validate_pd_series_func(function_name="force_2d")
        assert res == "'pd.Series' has no attribute 'force_2d'"

    def test_gpd(self):
        assert not transform.validate_pd_series_func(function_name="force_2d", geo=True)


class TestProcessors:
    proc = transform.ProcessingFunctions(TEST_DATASET_NAME)
    gdf = gpd.read_parquet(RESOURCES / TEST_DATA_DIR / "test.parquet")
    basic_df = pd.DataFrame({"a": [2, 3, 1], "b": ["b_1", "b_2", "c_3"]})
    df_a = pd.DataFrame({"a": [2, 3, 1]})
    messy_names_df = pd.DataFrame({"Column": [1, 2], "Two_Words": [3, 4]})
    dupe_df = pd.DataFrame({"a": [1, 1, 1, 2], "b": [3, 1, 3, 2]})
    whitespace_df = pd.DataFrame({"a": [2, 3, 1], "b": [" b_1 ", "  b_2", "c_3 "]})
    prev_df = pd.DataFrame({"a": [-1], "b": ["z"]})
    upsert_df = pd.DataFrame(
        {"a": [3, 2, 1], "b": ["d", "d", "d"], "c": [True, False, True]}
    )
    coerce_df = pd.DataFrame(
        {
            "simple_str": ["a", "b", "c"],
            "date_str": ["2024-01-01", None, None],
            "datetime_str": ["2024-01-01 12:57:01", None, None],
            "datetime_str_error": ["2024-01-01 12:57:01", "not a date", None],
            "numeric_str": ["1", None, "1.2"],
            "numeric_str_error": ["1", "not an int", "1.2"],
            "date": [date(2024, 1, 1), None, None],
            "datetime": [datetime(2024, 1, 1, 12, 57, 1), None, None],
            "numeric": [1, np.nan, 1.2],
            "integer_str": ["1", None, "2"],
            "integer_numeric": [1, np.nan, 2.0],
            "integer": pd.array([1, pd.NA, 2], dtype="Int32"),
        }
    )

    def test_reproject(self):
        assert self.gdf.crs.to_string() == "EPSG:4326"
        target = "EPSG:2263"
        reprojected = self.proc.reproject(self.gdf, target_crs=target)
        assert reprojected.crs.to_string() == target

    def test_sort(self):
        sorted = self.proc.sort(self.basic_df, by=["a"])
        expected = pd.DataFrame({"a": [1, 2, 3], "b": ["c_3", "b_1", "b_2"]})
        assert sorted.equals(expected)

    def test_filter_rows_equals(self):
        filtered = self.proc.filter_rows(
            self.basic_df, type="equals", column_name="a", val=1
        )
        expected = pd.DataFrame({"a": [1], "b": ["c_3"]})
        assert filtered.equals(expected)

    def test_filter_rows_contains(self):
        filtered = self.proc.filter_rows(
            self.basic_df, type="contains", column_name="b", val="b_"
        )
        expected = pd.DataFrame({"a": [2, 3], "b": ["b_1", "b_2"]})
        assert filtered.equals(expected)

    def test_filter_columns_keep(self):
        filtered = self.proc.filter_columns(self.basic_df, ["a"])
        assert filtered.equals(self.df_a)

    def test_filter_columns_drop(self):
        filtered = self.proc.filter_columns(self.basic_df, ["b"], mode="drop")
        assert filtered.equals(self.df_a)

    def test_rename_columns(self):
        renamed = self.proc.rename_columns(self.basic_df, {"a": "c"})
        expected = pd.DataFrame({"c": [2, 3, 1], "b": ["b_1", "b_2", "c_3"]})
        assert renamed.equals(expected)

    def test_rename_columns_drop(self):
        renamed = self.proc.rename_columns(self.basic_df, {"a": "c"}, drop_others=True)
        expected = pd.DataFrame({"c": [2, 3, 1]})
        assert renamed.equals(expected)

    def test_clean_column_names(self):
        cleaned = self.proc.clean_column_names(self.messy_names_df, replace={"_": "-"})
        expected = pd.DataFrame({"Column": [1, 2], "Two-Words": [3, 4]})
        assert cleaned.equals(expected)

    def test_clean_column_names_lower(self):
        cleaned = self.proc.clean_column_names(
            self.messy_names_df, replace={"_": "-"}, lower=True
        )
        expected = pd.DataFrame({"column": [1, 2], "two-words": [3, 4]})
        assert cleaned.equals(expected)

    def test_update_column(self):
        updated = self.proc.update_column(self.basic_df, column_name="a", val=5)
        expected = pd.DataFrame({"a": [5, 5, 5], "b": ["b_1", "b_2", "c_3"]})
        assert updated.equals(expected)

    @mock.patch("dcpy.connectors.edm.recipes.read_df")
    def test_append_prev(self, read_df):
        read_df.return_value = self.prev_df
        appended = self.proc.append_prev(self.basic_df)
        expected = pd.DataFrame({"a": [-1, 2, 3, 1], "b": ["z", "b_1", "b_2", "c_3"]})
        assert appended.equals(expected)

    @mock.patch("dcpy.connectors.edm.recipes.read_df")
    def test_upsert_column_of_previous_version(self, read_df):
        read_df.return_value = self.upsert_df
        upserted = self.proc.upsert_column_of_previous_version(self.basic_df, key=["a"])
        expected = pd.DataFrame(
            {"a": [3, 2, 1], "b": ["b_2", "b_1", "c_3"], "c": [True, False, True]}
        )
        assert upserted.equals(expected)

    def test_deduplicate(self):
        deduped = self.proc.deduplicate(self.dupe_df)
        expected = pd.DataFrame({"a": [1, 1, 2], "b": [3, 1, 2]})
        assert deduped.equals(expected)

    def test_deduplicate_by(self):
        deduped = self.proc.deduplicate(self.dupe_df, by="a")
        expected = pd.DataFrame({"a": [1, 2], "b": [3, 2]})
        assert deduped.equals(expected)

    def test_deduplicate_by_sort(self):
        deduped = self.proc.deduplicate(self.dupe_df, by="a", sort_columns="b")
        expected = pd.DataFrame({"a": [1, 2], "b": [1, 2]})
        assert deduped.equals(expected)

    def test_drop_columns(self):
        dropped = self.proc.drop_columns(self.basic_df, columns=["b"])
        expected = pd.DataFrame({"a": [2, 3, 1]})
        assert dropped.equals(expected)

    def test_strip_columns(self):
        stripped = self.proc.strip_columns(self.whitespace_df, ["b"])
        assert stripped.equals(self.basic_df)

    def test_strip_all_columns(self):
        stripped = self.proc.strip_columns(self.whitespace_df)
        assert stripped.equals(self.basic_df)

    @pytest.mark.parametrize(
        "original_column, cast, errors, expected_column",
        [
            # preserve columns when coercing to self
            ("simple_str", "string", None, "simple_str"),
            ("date", "date", None, "date"),
            ("datetime", "datetime", None, "datetime"),
            ("numeric", "numeric", None, "numeric"),
            # datetime conversions
            ("date_str", "date", None, "date"),
            ("datetime_str", "datetime", None, "datetime"),
            pytest.param(
                "datetime_str_error",
                "datetime",
                None,
                "datetime",
                marks=pytest.mark.xfail,
            ),
            ("datetime_str_error", "datetime", "coerce", "datetime"),
            # numeric conversions
            ("numeric_str", "numeric", None, "numeric"),
            pytest.param(
                "numeric_str_errors",
                "numeric",
                None,
                "numeric",
                marks=pytest.mark.xfail,
            ),
            ("numeric_str_error", "numeric", "coerce", "numeric"),
            # numeric to string conversion
            ("numeric", "string", None, "numeric_str"),
            # int conversions
            ("integer_str", "integer", None, "integer"),
            ("integer_numeric", "integer", None, "integer"),
        ],
    )
    def test_coerce_column_type(self, original_column, cast, errors, expected_column):
        errors = errors or "raise"
        coerced = self.proc.coerce_column_types(
            self.coerce_df, {original_column: cast}, errors=errors
        )
        assert coerced[original_column].equals(self.coerce_df[expected_column])

    def test_pd_series_func(self):
        transformed = self.proc.pd_series_func(
            self.basic_df, column_name="b", function_name="map", arg={"b_1": "c_1"}
        )
        expected = pd.DataFrame({"a": [2, 3, 1], "b": ["c_1", np.nan, np.nan]})
        assert transformed.equals(expected)

    def test_pd_series_func_str(self):
        transformed = self.proc.pd_series_func(
            self.basic_df,
            column_name="b",
            function_name="str.replace",
            pat="b_",
            repl="B-",
        )
        expected = pd.DataFrame({"a": [2, 3, 1], "b": ["B-1", "B-2", "c_3"]})
        assert transformed.equals(expected)

    def test_gpd_series_func(self):
        gdf = gpd.GeoDataFrame(
            {
                "a": [1, 2],
                "wkt": gpd.GeoSeries([None, Point(1, 2, 3)]),
            }
        )
        transformed = self.proc.pd_series_func(
            gdf, column_name="wkt", function_name="force_2d", geo=True
        )
        assert transformed.equals(
            gpd.GeoDataFrame(
                {
                    "a": [1, 2],
                    "wkt": gpd.GeoSeries([None, Point(1, 2)]),
                }
            )
        )

    def test_geoseries_on_non_gdf(self):
        with pytest.raises(
            TypeError, match="GeoSeries processing function specified for non-geo df"
        ):
            self.proc.pd_series_func(
                self.basic_df, column_name="wkt", function_name="force_2d", geo=True
            )

    def test_rename_geodataframe(self):
        transformed: gpd.GeoDataFrame = self.proc.rename_columns(
            self.gdf, map={"wkt": "geom"}
        )
        assert transformed.active_geometry_name == "geom"
        expected = gpd.read_parquet(RESOURCES / TEST_DATA_DIR / "renamed.parquet")
        assert transformed.equals(expected)

    def test_multi(self):
        gdf = gpd.GeoDataFrame(
            {
                "a": [1, 2, 3],
                "wkt": gpd.GeoSeries(
                    [
                        None,
                        Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
                        MultiPolygon(
                            [
                                Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
                                Polygon([(0, 0), (0, -1), (-1, 0), (0, 0)]),
                            ]
                        ),
                    ]
                ),
            }
        ).set_geometry("wkt")
        transformed = self.proc.multi(gdf)
        expected = gpd.GeoDataFrame(
            {
                "a": [1, 2, 3],
                "wkt": gpd.GeoSeries(
                    [
                        None,
                        MultiPolygon([Polygon([(0, 0), (0, 1), (1, 0), (0, 0)])]),
                        MultiPolygon(
                            [
                                Polygon([(0, 0), (0, 1), (1, 0), (0, 0)]),
                                Polygon([(0, 0), (0, -1), (-1, 0), (0, 0)]),
                            ]
                        ),
                    ]
                ),
            }
        )
        assert transformed.equals(expected)


def test_processing_no_steps(create_temp_filesystem: Path):
    input = RESOURCES / TEST_DATA_DIR / "test.parquet"
    output = create_temp_filesystem / "output.parquet"
    assert not output.exists(), (
        "Error in setup of test - output file should not exist yet"
    )

    transform.process(TEST_DATASET_NAME, [], [], input, output)
    assert output.exists()


def test_processing(create_temp_filesystem: Path):
    input = RESOURCES / TEST_DATA_DIR / "test.parquet"
    output = create_temp_filesystem / "output.parquet"
    assert not output.exists(), (
        "Error in setup of test - output file should not exist yet"
    )

    steps = [
        ProcessingStep(name="sort", args={"by": ["boro_code", "block", "lot"]}),
        ProcessingStep(name="rename_columns", args={"map": {"boro_code": "borough"}}),
    ]

    columns = [
        Column(id="borough", data_type="integer"),
        Column(id="block", data_type="integer"),
        Column(id="lot", data_type="integer"),
        Column(id="bbl", data_type="text"),
        Column(id="text", data_type="text"),
        Column(id="wkt", data_type="geometry"),
    ]

    transform.process(TEST_DATASET_NAME, steps, columns, input, output)
    assert output.exists()
    output_df = geoparquet.read_df(output)
    expected_df = geoparquet.read_df(TEST_OUTPUT)
    assert output_df.equals(expected_df)

    assert not (create_temp_filesystem / f"{TEST_DATASET_NAME}.csv").exists()
    transform.process(TEST_DATASET_NAME, steps, [], input, output, output_csv=True)
    assert (create_temp_filesystem / f"{TEST_DATASET_NAME}.csv").exists()


class TestValidateColumns:
    df = pd.DataFrame({"a": [2, 3, 1], "b": ["b_1", "b_2", "c_3"]})

    def test_validate_all_columns(self):
        transform.validate_columns(
            self.df,
            [Column(id="a", data_type="integer"), Column(id="b", data_type="text")],
        )

    def test_validate_partial_columns(self):
        transform.validate_columns(self.df, [Column(id="a", data_type="integer")])

    def test_validate_columns_fails(self):
        with pytest.raises(
            ValueError,
            match="defined in template but not found in processed dataset",
        ):
            transform.validate_columns(self.df, [Column(id="c", data_type="integer")])
