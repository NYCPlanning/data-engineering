import pytest

from dcpy.library.script import nycoc_checkbook


@pytest.mark.parametrize(
    "input_params, expected_output",
    [
        (
            {
                "type_of_data": "Spending_OGE",
                "max_records": "100",
                "records_from": "1",
                "response_columns": None,
                "search_criteria": None,
            },
            {
                "request": {
                    "type_of_data": "Spending_OGE",
                    "records_from": "1",
                    "max_records": "100",
                }
            },
        ),
        (
            {
                "type_of_data": "Spending",
                "max_records": "20000",
                "records_from": "100",
                "response_columns": ["column1", "column2"],
                "search_criteria": [
                    {"name": "column1", "type": "value", "value": "100"},
                    {"name": "column2", "type": "range", "start": "1", "end": "100"},
                ],
            },
            {
                "request": {
                    "type_of_data": "Spending",
                    "records_from": "100",
                    "max_records": "20000",
                    "response_columns": [{"column": "column1"}, {"column": "column2"}],
                    "search_criteria": [
                        {
                            "criteria": {
                                "name": "column1",
                                "type": "value",
                                "value": "100",
                            }
                        },
                        {
                            "criteria": {
                                "name": "column2",
                                "type": "range",
                                "start": "1",
                                "end": "100",
                            }
                        },
                    ],
                }
            },
        ),
    ],
)
def test_create_request_dict(input_params, expected_output):
    actual_output = nycoc_checkbook.create_request_dict(**input_params)
    assert actual_output == expected_output


def test_dict_to_xml_obj():
    request_dict = {
        "request": {
            "type_of_data": "Spending",
            "records_from": "100",
            "max_records": "20000",
            "response_columns": [{"column": "column1"}, {"column": "column2"}],
            "search_criteria": [
                {
                    "criteria": {
                        "name": "column2",
                        "type": "range",
                        "start": "1",
                        "end": "100",
                    }
                }
            ],
        }
    }
    xml_obj = nycoc_checkbook.dict_to_xml_obj(request_dict)

    assert "request" == xml_obj.tag
    assert len(request_dict["request"]) == len(xml_obj.findall("*"))
    assert len(request_dict["request"]["response_columns"]) == len(
        xml_obj.findall("./response_columns/")
    )


# TODO make this function more robust by testing for input/output date formats
def test_generate_monthly_ranges():
    assert nycoc_checkbook.generate_monthly_ranges("2023-2-1", "2023-2-7") == [
        ("2023-02-01", "2023-02-07")
    ]
    assert nycoc_checkbook.generate_monthly_ranges("2023-3-1", "2023-04-28") == [
        ("2023-03-01", "2023-03-31"),
        ("2023-04-01", "2023-04-28"),
    ]
    assert nycoc_checkbook.generate_monthly_ranges("2023-2-8", "2023-2-1") == [
        ("2023-02-01", "2023-02-08")
    ]
