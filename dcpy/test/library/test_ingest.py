import os
from unittest.mock import patch
from osgeo import gdal

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
        expected = "SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tthe_geom AS the_geom\nFROM field_names"
        assert format_field_names(self.ds, [], None, False, "csv") == expected

    def test_geom_dont_remove(self):
        expected = 'SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tthe_geom AS the_geom,\n\tGeometry AS "WKT"\nFROM field_names'
        assert format_field_names(self.ds, [], None, True, "csv") == expected

    def test_geom(self):
        expected = 'SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tGeometry AS "WKT"\nFROM field_names'
        assert (
            format_field_names(self.ds, [], None, True, "csv", ["the_geom"]) == expected
        )

    def test_with_fields(self):
        fields = ["col1", "col2", "geom"]
        expected = """SELECT\n\tColumn 1 AS col1,\n\tcol2 AS col2,\n\tthe_geom AS geom,\n\tGeometry AS "WKT"\nFROM field_names"""
        result = format_field_names(self.ds, fields, None, True, "csv", ["the_geom"])
        assert result == expected

    def test_with_sql_cte(self):
        sql = "WITH something AS (SELECT * FROM @filename WHERE col2 > 10) SELECT * FROM something"
        expected = (
            'WITH __cte__ AS (SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tGeometry AS "WKT"\nFROM field_names),\n'
            "something AS (SELECT * FROM __cte__ WHERE col2 > 10) SELECT * FROM something"
        )
        result = format_field_names(self.ds, [], sql, True, "csv", ["the_geom"])
        assert result == expected

    def test_with_sql_no_cte(self):
        sql = "SELECT * FROM @filename WHERE col2 > 10"
        expected = (
            "WITH __cte__ AS (SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tthe_geom AS the_geom\nFROM field_names)\n"
            "SELECT * FROM __cte__ WHERE col2 > 10"
        )
        result = format_field_names(self.ds, [], sql, False, "csv")
        assert result == expected

    def test_output_format_parquet(self):
        expected = 'SELECT\n\tColumn 1 AS column_1,\n\tcol2 AS col2,\n\tGeometry AS "geom"\nFROM field_names'
        result = format_field_names(self.ds, [], None, True, "parquet", ["the_geom"])
        assert result == expected


@patch("requests.get", side_effect=mock_request_get)
def test_ingest_with_sql(request_get):
    ingestor = Ingestor()
    ingestor.csv(get_config_file("bpl_libraries_sql"))


@patch("requests.get", side_effect=mock_request_get)
def test_script(request_get):
    ingestor = Ingestor()
    ingestor.csv(f"{template_path}/bpl_libraries.yml", version="test")
    assert os.path.isfile(".library/datasets/bpl_libraries/test/bpl_libraries.csv")
