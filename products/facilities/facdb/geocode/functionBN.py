import json

import pandas as pd
from tqdm.contrib.concurrent import process_map

from . import GeosupportError, g


class FunctionBN:
    def __init__(self, bin_field: str):
        self.bin_field = bin_field

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
        input_bin = inputs.get(self.bin_field)
        try:
            geo = g["BN"](bin=input_bin)
        except GeosupportError as e:
            geo = e.result

        geo = self.parser(geo)
        return dict(
            uid=uid,
            geo_bn=json.dumps(dict(inputs=dict(input_bin=input_bin), result=geo)),
        )

    def parser(self, geo):
        return dict(
            geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
                "BOROUGH BLOCK LOT (BBL)", None
            ),
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
