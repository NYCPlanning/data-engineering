from pandas import DataFrame


def truncate_to_single_row(ds_id, df: DataFrame):
    return df[0:1]
