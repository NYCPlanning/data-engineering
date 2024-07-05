import pandas as pd

from dcpy.utils import dataframe


def test_serialize_nested_objects():
    test_data = [
        {
            "boro_code": 4,
            "location": {"bbl": 4469310598},
            "details": {"text": "GsifrlkxmckyxrKHjGsr", "wkt": None},
        },
        {
            "boro_code": None,
            "location": {"bbl": 5192630318},
            "details": {"text": None, "wkt": "POINT (10.3894635 -175.008089)"},
        },
    ]
    df = pd.DataFrame(test_data)
    serialized_df = dataframe.serialize_nested_objects(df)

    # Check if nested structures are serialized as JSON strings
    for col in ["location", "details"]:
        for value in serialized_df[col]:
            assert isinstance(value, str)
            assert value.startswith("{")
