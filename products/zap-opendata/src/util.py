import pandas as pd


def timestamp_to_date(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    df[date_columns] = (
        df[date_columns]
        .apply(pd.to_datetime)
        .apply(lambda x: x.dt.strftime("%Y-%m-%d"))
    )
    return df
