from multiprocessing import Pool, cpu_count
from geosupport import Geosupport, GeosupportError
import requests
from datetime import datetime
import pandas as pd
import json
import os
import requests
from io import StringIO

g = Geosupport()


def get_bbl(inputs):
    BIN = inputs["bin"]
    try:
        geo = g["BN"](bin=BIN)
    except GeosupportError:
        try:
            geo = g["BN"](bin=BIN, mode="tpad")
        except GeosupportError as e2:
            geo = e2.result

    bbl = geo["BOROUGH BLOCK LOT (BBL)"]["BOROUGH BLOCK LOT (BBL)"]
    return {"bin": BIN, "bbl": bbl}


def get_bins(uid):
    # https://data.cityofnewyork.us/Housing-Development/Building-Footprints/nqwf-w8eh
    url = f'https://data.cityofnewyork.us/resource/{uid}.csv'
    headers = {'X-App-Token': os.environ['API_TOKEN']}
    params = {
        '$select': 'bin',
        '$limit': 50000000000000000
    }
    r = requests.get(f"{url}", headers=headers, params=params)
    return pd.read_csv(StringIO(r.text), index_col=False, dtype=str)


if __name__ == "__main__":
    metadata = requests.get(
        "https://data.cityofnewyork.us/api/views/nqwf-w8eh").json()
    uid = metadata["childViews"][1]
    df = get_bins(uid)
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(get_bbl, df.to_dict("records"), 100000)
    dff = pd.DataFrame(it)
    dff.to_csv('pluto_input_numbldgs.csv', index=False)
