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
    basic = """SELECT 
\tColumn 1 AS column_1,
\tcol2 AS col2,
\tthe_geom AS the_geom FROM field_names"""
    wkt = """SELECT 
\tColumn 1 AS column_1,
\tcol2 AS col2,
\tGeometry AS "WKT" FROM field_names"""

    def test_basic(self):
        assert format_field_names(self.ds, [], None, False, "csv") == self.basic

    def test_geom(self):
        print(format_field_names(self.ds, [], None, True, "csv"))
        assert format_field_names(self.ds, [], None, True, "csv") == self.basic


@patch("requests.get", side_effect=mock_request_get)
def test_ingest_with_sql(request_get):
    ingestor = Ingestor()
    ingestor.csv(get_config_file("bpl_libraries_sql"))


@patch("requests.get", side_effect=mock_request_get)
def test_script(request_get):
    ingestor = Ingestor()
    ingestor.csv(f"{template_path}/bpl_libraries.yml", version="test")
    assert os.path.isfile(".library/datasets/bpl_libraries/test/bpl_libraries.csv")
