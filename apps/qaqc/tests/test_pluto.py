import pytest
import pandas as pd
from src.pluto.helpers import get_data
from src.pluto.components.expected_value_differences_report import (
    ExpectedValueDifferencesReport,
)

TEST_S3_BUILD_FOLDER = "main"
TEST_VERSION_1 = "22v3.1"
TEST_VERSION_2 = "22v3"


@pytest.fixture
def example_ExpectedValueDifferencesReport():
    data = get_data(TEST_S3_BUILD_FOLDER)
    return ExpectedValueDifferencesReport(
        data=data["df_expected"],
        v1=TEST_VERSION_1,
        v2=TEST_VERSION_2,
    )


def test_v1_expected_records(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    v1_expected_records = example_ExpectedValueDifferencesReport.v1_expected_records
    assert isinstance(v1_expected_records, list)
    assert len(v1_expected_records) == 15


def test_values_by_field(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    field = "overlay1"
    values_by_field_expected = [
        None,
        "C1-2",
        "C2-3",
        "C2-1",
        "C2-2",
        "C1-1",
        "C1-5",
        "C2-4",
        "C2-5",
        "C1-4",
        "C1-3",
    ]
    v1_expected_records = example_ExpectedValueDifferencesReport.v1_expected_records
    values_by_field_actual = example_ExpectedValueDifferencesReport.values_by_field(
        v1_expected_records,
        field,
    )
    assert values_by_field_actual == values_by_field_expected


def test_values_by_fields(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    fields = ["overlay1", "overlay2"]
    expected_values = [
        None,
        "C1-2",
        "C2-3",
        "C2-1",
        "C2-2",
        "C1-1",
        "C1-5",
        "C2-4",
        "C2-5",
        "C1-4",
        "C1-3",
        None,
        "C1-2",
        "C2-3",
        "C2-1",
        "C2-2",
        "C1-1",
        "C1-5",
        "C2-4",
        "C2-5",
        "C1-4",
        "C1-3",
    ]
    v1_expected_records = example_ExpectedValueDifferencesReport.v1_expected_records
    actual_values = example_ExpectedValueDifferencesReport.values_by_fields(
        v1_expected_records,
        fields,
    )
    assert actual_values == expected_values


def test_value_differences_across_versions(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    comparison_name = "zoning"
    expeted_in1not2 = ['M1-4/R9', 'M1-4/R7-3', 'M1-4/R9', 'M1-4/R7-3']
    expeted_in2not1 = []
    (
        in1not2,
        in2not1,
    ) = example_ExpectedValueDifferencesReport.value_differences_across_versions(
        comparison_name
    )
    assert in1not2 == expeted_in1not2
    assert in2not1 == expeted_in2not1
