from dcpy.utils.logging import logger
from typing import List
import pandas as pd

from os.path import exists
from pathlib import Path
from ingest.PUMS.PUMS_data import PUMSData


allowed_HVS_cache_types = [".csv", ".pkl"]


def load_PUMS(
    year: int,
    variable_types: List = ["demographics"],
    include_rw: bool = True,
    limited_PUMA: bool = False,
    household: bool = False,
    return_ingestor=False,
    requery: bool = False,
):
    Path("data/").mkdir(exist_ok=True)

    cache_path = PUMSData.get_cache_fn(
        variable_types=variable_types,
        limited_PUMA=limited_PUMA,
        year=year,
        include_rw=include_rw,
    )
    if requery or not exists(cache_path):
        logger.info(f"Making get request to generate data sent to {cache_path}")
        ingestor = PUMSData(
            variable_types=variable_types,
            year=year,
            limited_PUMA=limited_PUMA,
            include_rw=include_rw,
            household=household,
        )
    else:
        print(f"reading raw data from cache path {cache_path}")
    PUMS_data = pd.read_pickle(cache_path)
    if return_ingestor:
        rv = ingestor
    else:
        rv = PUMS_data
    logger.info(
        f"PUMS data with {PUMS_data.shape[0]} records loaded, ready for aggregation"
    )
    return rv
