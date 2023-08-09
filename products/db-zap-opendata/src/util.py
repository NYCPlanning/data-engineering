import logging
import os
import pandas as pd


def create_logger(logger_name, file_name) -> logging.Logger:
    if not os.path.exists(".logs/"):
        os.makedirs(".logs/")
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s:%(name)s: %(message)s")
    logger.handlers = []
    file_handler = logging.FileHandler(f".logs/{file_name}")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(file_handler)
    return logger


def timestamp_to_date(df: pd.DataFrame, date_columns: list) -> pd.DataFrame:
    df[date_columns] = (
        df[date_columns]
        .apply(pd.to_datetime)
        .apply(lambda x: x.dt.strftime("%Y-%m-%d"))
    )
    return df
