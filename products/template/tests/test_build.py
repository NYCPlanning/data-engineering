# These are tests to be run after a full build
import pytest
from dcpy.utils import s3
from dcpy.connectors.edm import publishing

from build_scripts import PRODUCT_S3_NAME, BUILD_NAME, PG_CLIENT


# * test Load stage
@pytest.mark.end_to_end()
def test_load_db_tables():
    expected_source_tables = [
        "nypl_libraries",
        "bpl_libraries",
        "qpl_libraries",
        "dpr_parksproperties",
        "dpr_greenthumb",
        "lpc_landmarks",
    ]
    actual_tables = PG_CLIENT.get_schema_tables()
    for table in expected_source_tables:
        assert table in actual_tables


# * test Transform stage
@pytest.mark.end_to_end()
def test_transform_staging():
    expected_build_tables = [
        "stg_nypl_libraries",
        "stg_bpl_libraries",
        "stg_qpl_libraries",
        "stg_dpr_parksproperties",
        "stg_dpr_greenthumb",
        "boroughs",
    ]
    actual_tables = PG_CLIENT.get_schema_tables()
    for table in expected_build_tables:
        assert table in actual_tables


@pytest.mark.end_to_end()
def test_transform_outputs():
    expected_build_tables = [
        "libraries",
        "green_spaces",
        "historic_landmarks",
        "templatedb",
    ]
    actual_tables = PG_CLIENT.get_schema_tables()
    for table in expected_build_tables:
        assert table in actual_tables


@pytest.mark.end_to_end()
def test_transform_aggregations():
    expected_build_tables = [
        "templatedb_boroughs",
    ]
    actual_tables = PG_CLIENT.get_schema_tables()
    for table in expected_build_tables:
        assert table in actual_tables


# * test Export stage
@pytest.mark.end_to_end()
def test_export_draft():
    expected_build_name = BUILD_NAME
    actual_build_name = publishing.get_draft_builds(product=PRODUCT_S3_NAME)

    assert expected_build_name in actual_build_name


@pytest.mark.end_to_end()
def test_export_files():
    expected_export_file_names = [
        "source_data_versions.csv",
        "templatedb.csv",
    ]
    actual_files = s3.get_filenames(
        publishing.BUCKET, f"{PRODUCT_S3_NAME}/draft/{BUILD_NAME}/output"
    )
    for file_name in expected_export_file_names:
        assert file_name in actual_files
