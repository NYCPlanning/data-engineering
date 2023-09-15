# These are tests to be run after a full build
import pytest
from dcpy.utils.postgres import get_schema_tables
from build_scripts import BUILD_SCHEMA


# test build stage Load
@pytest.mark.end_to_end()
def test_load_db_tables():
    expected_source_tables = [
        "dcp_cb2020_wi",
        "dcp_cdboundaries_wi",
        "dcp_councildistricts_wi",
        "dcp_facilities",
        "dof_shoreline",
        "dpr_greenthumb",
        "lpc_historic_districts",
        "lpc_landmarks",
    ]
    actual_source_tables = get_schema_tables(BUILD_SCHEMA)

    assert actual_source_tables == expected_source_tables


@pytest.mark.skip(reason="TODO")
@pytest.mark.end_to_end()
def test_load_other_stuff():
    assert False


# test build stage Transform
@pytest.mark.skip(reason="TODO")
@pytest.mark.end_to_end()
def test_transform():
    assert False
