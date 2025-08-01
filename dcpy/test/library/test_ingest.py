import os
from unittest.mock import patch
from osgeo import gdal
import pandas as pd

from dcpy.library.ingest import Ingestor, format_field_names
from dcpy.test.conftest import mock_request_get

from . import (
    get_config_file,
    test_root_path,
    template_path,
)


class TestFormatFieldNames:
    ds = gdal.OpenEx(
        test_root_path / "data" / "field_names.csv",
        gdal.OF_VECTOR,
        open_options=["GEOM_POSSIBLE_NAMES=the_geom"],
    )

    def test_basic(self):
        expected = 'SELECT\n\t"Column 1" AS "column_1",\n\t"col2" AS "col2",\n\t"the_geom" AS "the_geom"\nFROM field_names'
        assert format_field_names(self.ds, [], None, False, "CSV") == expected

    def test_geom_dont_remove(self):
        expected = 'SELECT\n\t"Column 1" AS "column_1",\n\t"col2" AS "col2",\n\t"the_geom" AS "the_geom",\n\t"Geometry" AS "WKT"\nFROM field_names'
        assert format_field_names(self.ds, [], None, True, "CSV") == expected

    def test_geom(self):
        expected = 'SELECT\n\t"Column 1" AS "column_1",\n\t"col2" AS "col2",\n\t"Geometry" AS "WKT"\nFROM field_names'
        assert (
            format_field_names(self.ds, [], None, True, "CSV", ["the_geom"]) == expected
        )

    def test_with_fields(self):
        fields = ["col1", "col2", "geom"]
        expected = """SELECT\n\t"Column 1" AS "col1",\n\t"col2" AS "col2",\n\t"the_geom" AS "geom",\n\t"Geometry" AS "WKT"\nFROM field_names"""
        result = format_field_names(self.ds, fields, None, True, "CSV")
        assert result == expected

    def test_with_sql_cte(self):
        sql = "WITH something AS (SELECT * FROM @filename WHERE col2 > 10) SELECT * FROM something"
        expected = (
            'WITH __cte__ AS (SELECT\n\t"Column 1" AS "column_1",\n\t"col2" AS "col2",\n\t"Geometry" AS "WKT"\nFROM field_names),\n'
            "something AS (SELECT * FROM __cte__ WHERE col2 > 10) SELECT * FROM something"
        )
        result = format_field_names(self.ds, [], sql, True, "CSV", ["the_geom"])
        assert result == expected

    def test_with_sql_no_cte(self):
        sql = "SELECT * FROM @filename WHERE col2 > 10"
        expected = (
            'WITH __cte__ AS (SELECT\n\t"Column 1" AS "column_1",\n\t'
            '"col2" AS "col2",\n\t"the_geom" AS "the_geom"\nFROM field_names)\n'
            "SELECT * FROM __cte__ WHERE col2 > 10"
        )
        result = format_field_names(self.ds, [], sql, False, "CSV")
        assert result == expected

    def test_output_format_parquet(self):
        expected = 'SELECT\n\t"Column 1" AS "column_1",\n\t"col2" AS "col2",\n\t"Geometry" AS "geom"\nFROM field_names'
        result = format_field_names(self.ds, [], None, True, "Parquet", ["the_geom"])
        assert result == expected


class TestIngestor:
    """Slightly more integration-y tests for the Ingestor class."""

    test_dataset = "field_names"
    template = get_config_file(test_dataset)
    output_file_csv = f".library/datasets/{test_dataset}/test/{test_dataset}.csv"
    output_file_parquet = (
        f".library/datasets/{test_dataset}/test/{test_dataset}.parquet"
    )
    ingestor = Ingestor()

    def test_creates_output_successfully(self):
        print(self.template)
        self.ingestor.csv(self.template)
        assert os.path.isfile(self.output_file_csv)

    def test_output_columns_csv(self):
        self.ingestor.csv(self.template)
        df = pd.read_csv(self.output_file_csv)
        assert list(df.columns) == ["WKT", "column_1", "col2"]

    def test_output_columns_parquet(self):
        self.ingestor.parquet(self.template)
        df = pd.read_parquet(self.output_file_parquet)
        assert list(df.columns) == ["column_1", "col2", "geom", "geom_bbox"]

    @patch("requests.get", side_effect=mock_request_get)
    def test_script(self, request_get):
        self.ingestor.csv(f"{template_path}/bpl_libraries.yml", version="test")
        assert os.path.isfile(".library/datasets/bpl_libraries/test/bpl_libraries.csv")
