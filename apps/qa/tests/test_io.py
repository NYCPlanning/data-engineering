# test s3 and sql data IO
from dcpy.utils.postgres import PostgresClient
from dcpy.connectors.edm.recipes import get_config
from src import QAQC_DB, QAQC_DB_SCHEMA_SOURCE_DATA

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
    dataset_confg = get_config(TEST_DATA_SOURCE_NAME, TEST_DATA_SOURCE_VERSION)
    assert isinstance(dataset_confg, dict)
    assert dataset_confg["dataset"]["name"] == TEST_DATA_SOURCE_NAME
    assert dataset_confg["dataset"]["version"] == TEST_DATA_SOURCE_VERSION


def test_source_data_columns():
    pg_client = PostgresClient(database=QAQC_DB, schema=QAQC_DB_SCHEMA_SOURCE_DATA)
    columns = pg_client.get_table_columns(
        table_name=f"{TEST_DATA_SOURCE_NAME}_{TEST_DATA_SOURCE_VERSION}",
    )
    assert columns == TEST_DATA_SOURCE_COLUMNS
