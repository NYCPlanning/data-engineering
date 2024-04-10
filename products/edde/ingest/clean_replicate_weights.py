"""All data should have same column labels for replicate weights columns"""
import pandas as pd

rw_cols_clean = [f"rw_{i}" for i in range(1, 81)]


def HVS_rep_weights_clean(df: pd.DataFrame) -> pd.DataFrame:
    mapper = {f"fw{i}": rw_cols_clean[i - 1] for i in range(1, 81)}
    df.rename(columns=mapper, inplace=True)
    return df
