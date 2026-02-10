import pytest
from src.pages.pluto.components.expected_value_differences_report import (
    ExpectedValueDifferencesReport,
)
from src.pages.pluto.helpers import PRODUCT, get_data

from dcpy.connectors.edm import publishing

TEST_VERSION_1 = "23v1"
TEST_VERSION_2 = "22v3"


@pytest.fixture
def example_ExpectedValueDifferencesReport():
    data = get_data(publishing.PublishKey(PRODUCT, TEST_VERSION_1))
    return ExpectedValueDifferencesReport(
        data=data["df_expected"],
        v=TEST_VERSION_1,
        v_prev=TEST_VERSION_2,
    )


def test_expected_records(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    expected_records = example_ExpectedValueDifferencesReport.v_expected_records
    assert isinstance(expected_records, list)
    assert len(expected_records) == 15


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
        "C1-4",
        "C2-5",
        "C1-3",
    ]
    expected_records = example_ExpectedValueDifferencesReport.v_expected_records
    values_by_field_actual = example_ExpectedValueDifferencesReport.values_by_field(
        expected_records,
        field,
    )
    assert values_by_field_actual == values_by_field_expected


def test_values_by_fields(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    fields = ["overlay1", "overlay2"]
    expected_values = [
        None,
        "C1-1",
        "C1-2",
        "C1-3",
        "C1-4",
        "C1-5",
        "C2-1",
        "C2-2",
        "C2-3",
        "C2-4",
        "C2-5",
    ]
    expected_records = example_ExpectedValueDifferencesReport.v_expected_records
    actual_values = example_ExpectedValueDifferencesReport.values_by_fields(
        expected_records,
        fields,
    )
    assert set(actual_values) == set(expected_values)


def test_value_differences_across_versions(
    example_ExpectedValueDifferencesReport: ExpectedValueDifferencesReport,
):
    comparison_name = "zoning"
    expected_in1not2 = ["M1-4/R7-3", "M1-4/R9"]
    expected_in2not1: list[str] = []
    (
        in1not2,
        in2not1,
    ) = example_ExpectedValueDifferencesReport.value_differences_across_versions(
        comparison_name
    )
    assert in1not2 == expected_in1not2
    assert in2not1 == expected_in2not1
