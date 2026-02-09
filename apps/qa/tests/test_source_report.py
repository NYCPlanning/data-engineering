# test generation of source data reports
import pandas as pd
import pytest
from src import QAQC_DB, QAQC_DB_SCHEMA_SOURCE_DATA
from src.shared.constants import DATASET_NAMES
from src.shared.utils.source_report import (
    compare_source_data_columns,
    compare_source_data_row_count,
    get_source_dataset_ids,
)

from dcpy.connectors.edm import publishing
from dcpy.utils.postgres import PostgresClient

REFERENCE_VESION = "2023-04-01"

TEST_DATASET_NAME = DATASET_NAMES["ztl"]
TEST_DATASET_REFERENCE_VERSION = REFERENCE_VESION
TEST_DATA_SOURCE_NAME = "dcp_zoningmapamendments"
TEST_DATA_SOURCE_VERSION_REFERENCE = "20230404"
TEST_DATA_SOURCE_VERSION_LATEST = "20230724"
TEST_DATA_SOURCE_NAMES = [
    "dcp_commercialoverlay",
    "dcp_limitedheight",
    "dcp_specialpurpose",
    "dcp_specialpurposesubdistricts",
    "dcp_zoningdistricts",
    "dcp_zoningmapamendments",
    "dcp_zoningmapindex",
    "dof_dtm",
]
TEST_SOURCE_REPORT_RESULTS = {
    TEST_DATA_SOURCE_NAME: {
        "version_reference": "20230306",
        "version_latest": "20230404",
    }
}


def test_get_source_data_versions_from_build():
    source_data_versions = publishing.get_source_data_versions(
        publishing.PublishKey(TEST_DATASET_NAME, TEST_DATASET_REFERENCE_VERSION)
    )
    assert isinstance(source_data_versions, pd.DataFrame)
    assert (
        source_data_versions.loc[TEST_DATA_SOURCE_NAME, "version"]
        == TEST_DATA_SOURCE_VERSION_REFERENCE
    )


def test_get_source_dataset_names():
    source_dataset_names = get_source_dataset_ids(
        publishing.PublishKey(TEST_DATASET_NAME, REFERENCE_VESION)
    )
    assert source_dataset_names == TEST_DATA_SOURCE_NAMES


def test_compare_source_data_columns():
    pg_client = PostgresClient(database=QAQC_DB, schema=QAQC_DB_SCHEMA_SOURCE_DATA)
    source_report_results = compare_source_data_columns(
        TEST_SOURCE_REPORT_RESULTS, pg_client=pg_client
    )
    assert isinstance(
        source_report_results[TEST_DATA_SOURCE_NAME]["same_columns"], bool
    )
    assert source_report_results[TEST_DATA_SOURCE_NAME]["same_columns"] is True


@pytest.mark.skip(reason="requires mock data")
def test_compare_source_data_row_count():
    pg_client = PostgresClient(database=QAQC_DB, schema=QAQC_DB_SCHEMA_SOURCE_DATA)
    source_report_results = compare_source_data_row_count(
        TEST_SOURCE_REPORT_RESULTS, pg_client=pg_client
    )
    assert isinstance(
        source_report_results[TEST_DATA_SOURCE_NAME]["same_row_count"], bool
    )
    assert source_report_results[TEST_DATA_SOURCE_NAME]["same_row_count"] is False
