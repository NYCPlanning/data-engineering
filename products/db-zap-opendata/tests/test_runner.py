import os
import pytest
from collections import namedtuple
import pandas as pd

from src.runner import Runner

TEST_SCHEMA_SUFFIX = os.environ.get("TEST_SCHEMA_SUFFIX", None)

test_schema_expected = "test_data_expected"
test_schema_actual = "test_data_actual"
if TEST_SCHEMA_SUFFIX:
    test_schema_actual = f"{test_schema_actual}_{TEST_SCHEMA_SUFFIX}"

test_data_query = """
    select * from :dataset_name :filter_clause
"""
TestDataset = namedtuple(
    "TestDataset",
    [
        "table_name",
        "filter_clause",
        "expected_row_count",
    ],
)
# TODO add all output table names to these test datasets
test_datasets = [
    TestDataset(
        table_name="dcp_projects",
        filter_clause="""
            where dcp_name in (
                'P2016K0159', '2023K0228', 'P2005K0122', '2021M0260'
                )
            """,
        expected_row_count=4,
    ),
    TestDataset(
        table_name="dcp_projectbbls",
        filter_clause="""
            where SUBSTRING(dcp_name, 0,10) in (
                'P2016K0159', '2023K0228', 'P2005K0122', '2021M0260'
                )
            """,
        expected_row_count=2210,
    ),
]


@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_validate_expected_test_data(test_dataset):
    runner = Runner(name=test_dataset.table_name, schema=test_schema_expected)
    test_data_expected = runner.pg.execute_select_query(
        base_query=test_data_query,
        parameters={
            "dataset_name": test_dataset.table_name,
            "filter_clause": test_dataset.filter_clause,
        },
    )
    assert len(test_data_expected) == test_dataset.expected_row_count


@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_download(test_dataset):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner.download()
    # TODO assert things


@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_combine(test_dataset):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner.combine()

    table_name = f"{test_dataset.table_name}_crm"

    test_data_query_parameters = {
        "dataset_name": table_name,
        "filter_clause": test_dataset.filter_clause,
    }
    test_data_actual = (
        runner.pg.execute_select_query(
            base_query=test_data_query,
            parameters=test_data_query_parameters,
        )
        .sort_index(axis=1)
        .sort_values(by="@odata.etag")
        .reset_index()
    )
    # TODO assert things
    # runner_expected = Runner(name=test_dataset.table_name, schema=test_data_expected_schema)
    # test_data_expected = runner_expected.pg.execute_select_query(
    #     base_query=test_data_query,
    #     parameters=test_data_query_parameters,
    # ).sort_index(axis=1).sort_values(by="@odata.etag").reset_index()
    # pd.testing.assert_frame_equal(test_data_actual, test_data_expected)


@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_recode(test_dataset):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner.recode()


@pytest.mark.integration()
# HACK skipping dcp_projects because Runner.recode_id() takes ~1 hour
# @pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_recode_id(test_dataset=test_datasets[1]):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner.recode_id()
    # TODO assert things


@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_export(test_dataset):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner.export()
    # TODO assert things


@pytest.mark.skip(reason="Runner.recode_id() takes ~1 hour")
@pytest.mark.integration()
@pytest.mark.parametrize("test_dataset", test_datasets)
def test_runner_main(test_dataset):
    runner = Runner(
        name=test_dataset.table_name,
        schema=test_schema_actual,
    )
    runner()
    # TODO assert things for all output tables
