"""To do
Medium term:
Integrate some component on an existing github workflow to this project.
Doing something on commit like linting would be a good place to start

Longer Term:
Put age into buckets
Start aggregation with replicated weights
"""

from dcpy.utils.logging import logger
import requests
import pandas as pd
import time


def make_GET_request(url: str, request_name: str) -> pd.DataFrame:
    start_time = time.perf_counter()
    logger.info(f"GET url for {request_name} is {url}")
    counter = 0
    res = None
    req_limit = 5
    while (res is None or res.status_code != 200) and counter < req_limit:
        res = requests.get(url)
        counter += 1

    if res.status_code != 200:
        logger.error(f"{req_limit} attempts made for for {request_name}: {res.text}")
        raise Exception(f"error making GET request for {request_name}: {res.text}")
    end_time = time.perf_counter()
    logger.info(f"this get request took {end_time - start_time} seconds")
    return response_to_df(res.json())


def response_to_df(res_json):
    return pd.DataFrame(data=res_json[1:], columns=res_json[0])
