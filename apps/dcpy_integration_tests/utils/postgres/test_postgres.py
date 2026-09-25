import uuid

import pandas as pd
import pytest

SAMPLE_TABLE_NAME = "test_table"
TEST_DATA = pd.DataFrame(
    {"name": ["test1", "test2", "test3"], "value": [100, 200, 300]}
)


@pytest.fixture
def source_schema_with_sample_table(pg_client):
    test_schema = f"test_source_schema_{uuid.uuid4().hex[:8]}"

    pg_client.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{test_schema}"')
    pg_client.execute_query(f'''
        CREATE TABLE "{test_schema}"."{SAMPLE_TABLE_NAME}" (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    ''')

    pg_client.insert_dataframe(TEST_DATA, SAMPLE_TABLE_NAME, schema=test_schema)

    yield test_schema

    pg_client.execute_query(f'DROP SCHEMA IF EXISTS "{test_schema}" CASCADE')


def test_creating_a_view_from_another_schema(
    pg_client, source_schema_with_sample_table
):
    result_table_name = pg_client.create_view(
        SAMPLE_TABLE_NAME, source_schema_with_sample_table
    )
    assert pg_client.is_view(SAMPLE_TABLE_NAME), "The created entity should be a view."
    assert result_table_name == SAMPLE_TABLE_NAME, (
        "The table's name should match the source table."
    )

    view_data = pg_client.execute_select_query(
        f'SELECT name, value FROM "{SAMPLE_TABLE_NAME}" ORDER BY name'
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(view_data, TEST_DATA)


def test_copying_a_table_from_another_schema(
    pg_client, source_schema_with_sample_table
):
    result_table_name = pg_client.copy_table(
        SAMPLE_TABLE_NAME, source_schema_with_sample_table
    )
    assert not pg_client.is_view(SAMPLE_TABLE_NAME), (
        "The created entity should be a table."
    )
    assert result_table_name == SAMPLE_TABLE_NAME, (
        "The view's name should match the source table."
    )

    view_data = pg_client.execute_select_query(
        f'SELECT name, value FROM "{SAMPLE_TABLE_NAME}" ORDER BY name'
    ).reset_index(drop=True)
    pd.testing.assert_frame_equal(view_data, TEST_DATA)
