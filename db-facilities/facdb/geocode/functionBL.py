import json
from functools import wraps

import pandas as pd
from tqdm.contrib.concurrent import process_map

from . import GeosupportError, g


class FunctionBL:
    def __init__(self, bbl_field: str = None):
        self.bbl_field = bbl_field

    def geocode_a_dataframe(self, df: pd.DataFrame):
        records = df.to_dict("records")
        it = process_map(self.geocode_one_record, records, chunksize=1000)
        df_geo = pd.DataFrame(it)
        return df.merge(df_geo, how="left", on="uid", suffixes=("", "_"))

    def geocode_one_record(self, inputs: dict) -> dict:
        """
        Note that df needs
        """
        uid = inputs.get("uid")
        input_bbl = inputs.get(self.bbl_field)
        try:
            geo = g["BL"](bbl=input_bbl)
        except GeosupportError as e:
            geo = e.result

        geo = self.parser(geo)
        return dict(
            uid=uid,
            geo_bl=json.dumps(dict(inputs=dict(input_bbl=input_bbl), result=geo)),
        )

    def parser(self, geo):
        return dict(
            geo_bin=geo.get("Building Identification Number (BIN)", None),
            geo_latitude=geo.get("Latitude", None),
            geo_longitude=geo.get("Longitude", None),
            geo_grc=geo.get("Geosupport Return Code (GRC)", None),
            geo_reason_code=geo.get("Reason Code", None),
            geo_message=geo.get("Message", "msg err"),
        )

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            df = func()
            df = self.geocode_a_dataframe(df)
            return df

        return wrapper
