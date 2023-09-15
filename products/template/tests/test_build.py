import pytest
from dcpy.utils.postgres import get_schema_tables
from .. import BUILD_SCHEMA


# test build stage Load
@pytest.mark.end_to_end()
def test_load_db_tables():
    expected_source_tables = [
        "dcp_cb2020_wi",
        "dcp_cdboundaries_wi",
        "dcp_councildistricts_wi",
        "dcp_facilities",
        "dcp_pluto",
        "dof_shoreline",
        "dpr_greenthumb",
        "lpc_historic_districts",
        "lpc_landmarks",
    ]
    built_in_tables = [
        "spatial_ref_sys",
        "geography_columns",
        "geometry_columns",
    ]
    all_build_db_tables = sorted(get_schema_tables(BUILD_SCHEMA))
    actual_source_tables = [
        table_name
        for table_name in all_build_db_tables
        if table_name not in built_in_tables
    ]
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
