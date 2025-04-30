"""Access to ingestion code"""

from typing import List
import pandas as pd

from os.path import exists
from pathlib import Path
from ingest.PUMS.PUMS_data import PUMSData
from ingest.HVS.HVS_ingestion import create_HVS
from ingest.make_cache_fn import make_HVS_cache_fn

from utils.make_logger import create_logger
from utils.wd_management import correct_wd

logger = create_logger("load_data_logger", "logs/load_data.log")

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
    assert correct_wd(), "Code is not being run from root directory"
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


def load_HVS(
    year: int,
    requery: bool = False,
    human_readable: bool = False,
    output_type: str = ".csv",
) -> pd.DataFrame:
    """
    To-do:
    - break out pums download into it's own function that can be called on its own
    - remove return_ingestor flag and always get ingestor object

    :param limited_PUMA: only query for first PUMA in each borough. For debugging
    :return: pandas dataframe of PUMS data
    """
    assert correct_wd(), "Code is not being run from root directory"
    Path("data/").mkdir(exist_ok=True)

    if output_type not in allowed_HVS_cache_types:
        raise Exception(
            f"{output_type} file type not supported for HVS cache. Allowed file types are {allowed_HVS_cache_types} "
        )
    HVS_cache_path = make_HVS_cache_fn(
        year=year, human_readable=human_readable, output_type=output_type
    )
    if requery or not exists(HVS_cache_path):
        create_HVS(year=year, human_readable=human_readable, output_type=output_type)

    if output_type == ".pkl":
        HVS_data = pd.read_pickle(HVS_cache_path)
    elif output_type == ".csv":
        HVS_data = pd.read_csv(HVS_cache_path)
    HVS_data
    logger.info(
        f"HVS data with {HVS_data.shape[0]} records loaded, ready for aggregation"
    )
    return HVS_data
