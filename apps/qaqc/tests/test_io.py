# test s3 and sql data IO
from dcpy.connectors.postgres import get_table_columns
from src import QAQC_DB_SCHEMA_SOURCE_DATA
from src.digital_ocean_utils import get_datatset_config

TEST_DATA_SOURCE_NAME = "dcp_zoningmapamendments"
TEST_DATA_SOURCE_VERSION = "20230404"
TEST_DATA_SOURCE_COLUMNS = [
    "effective",
    "lucats",
    "ogc_fid",
    "project_na",
    "shape_area",
    "shape_leng",
    "status",
    "ulurpno",
    "wkb_geometry",
]


def test_dataset_config():
    dataset_confg = get_datatset_config(
        dataset=TEST_DATA_SOURCE_NAME, version=TEST_DATA_SOURCE_VERSION
    )
    assert isinstance(dataset_confg, dict)
    assert dataset_confg["dataset"]["name"] == TEST_DATA_SOURCE_NAME
    assert dataset_confg["dataset"]["version"] == TEST_DATA_SOURCE_VERSION


def test_get_table_columns():
    columns = get_table_columns(
        table_schema=QAQC_DB_SCHEMA_SOURCE_DATA,
        table_name=f"{TEST_DATA_SOURCE_NAME}_{TEST_DATA_SOURCE_VERSION}",
    )
    assert columns == TEST_DATA_SOURCE_COLUMNS
