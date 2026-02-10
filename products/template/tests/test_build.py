# These are tests to be run after a full build
import pytest
from build_scripts import BUILD_KEY, PG_CLIENT

from dcpy.connectors.edm import publishing


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


# * test Export stage
@pytest.mark.end_to_end()
def test_export_build():
    assert BUILD_KEY.build in publishing.get_builds(product=BUILD_KEY.product)


@pytest.mark.end_to_end()
def test_export_files():
    expected_export_file_names = {
        "source_data_versions.csv",
        "build_metadata.json",
        "data_dictionary.yml",
        "data_dictionary.pdf",
        "data_dictionary.xlsx",
        "templatedb.csv",
        "output.zip",
    }
    actual_files = publishing.get_filenames(BUILD_KEY)
    assert actual_files == expected_export_file_names
