import pandas as pd
from src.util import timestamp_to_date


def test_timestamp_to_date(test_data_path):
    data = pd.read_csv(f"{test_data_path}/timestamp_data.csv")
    data_dates = timestamp_to_date(
        data, date_columns=["date_column_a", "date_column_b"]
    )

    assert data_dates["date_column_a"].equals(data_dates["date_column_a_expected"])
    assert data_dates["date_column_b"].equals(data_dates["date_column_b_expected"])
