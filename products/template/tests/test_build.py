# These are tests to be run after a full build
import pytest
from dcpy.utils import postgres, git

BUILD_SCHEMA = git.run_name()
pg_client = postgres.PostgresClient(
    schema=BUILD_SCHEMA,
)


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
        "nyc_landmarks",
    ]
    actual_tables = pg_client.get_schema_tables()
    for table in expected_source_tables:
        assert table in actual_tables


@pytest.mark.skip(reason="TODO")
@pytest.mark.end_to_end()
def test_load_other_stuff():
    assert False


# test build stage Transform
@pytest.mark.skip(reason="TODO")
@pytest.mark.end_to_end()
def test_transform():
    expected_build_tables = [
        "hi_new_table",
    ]
    actual_tables = pg_client.get_schema_tables()
    for table in expected_build_tables:
        assert table in actual_tables
