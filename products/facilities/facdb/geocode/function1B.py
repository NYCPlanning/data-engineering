import json

import pandas as pd
from tqdm.contrib.concurrent import process_map

from . import GeosupportError, g


class Function1B:
    def __init__(
        self,
        street_name_field: str | None = None,
        house_number_field: str | None = None,
        borough_field: str | None = None,
        zipcode_field: str | None = None,
    ):
        self.street_name_field = street_name_field
        self.house_number_field = house_number_field
        self.borough_field = borough_field
        self.zipcode_field = zipcode_field

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
        input_sname = inputs.get(self.street_name_field)
        input_hnum = inputs.get(self.house_number_field)
        input_borough = inputs.get(self.borough_field)
        input_zipcode = inputs.get(self.zipcode_field)
        try:
            geo = g["1B"](
                street_name=input_sname,
                house_number=input_hnum,
                borough=input_borough,
                zip_code=input_zipcode,
            )
        except GeosupportError as e:
            geo = e.result

        geo = self.parser(geo)
        return dict(
            uid=uid,
            geo_1b=json.dumps(
                dict(
                    inputs=dict(
                        input_sname=input_sname,
                        input_hnum=input_hnum,
                        input_borough=input_borough,
                        input_zipcode=input_zipcode,
                    ),
                    result=geo,
                )
            ),
        )

    def parser(self, geo):
        return dict(
            geo_house_number=geo.get("House Number - Display Format", None),
            geo_street_name=geo.get("First Street Name Normalized", None),
            geo_borough_code=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
                "Borough Code", None
            ),
            geo_zip_code=geo.get("ZIP Code", None),
            geo_bin=geo.get(
                "Building Identification Number (BIN) of Input Address or NAP", None
            ),
            geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
                "BOROUGH BLOCK LOT (BBL)", None
            ),
            geo_latitude=geo.get("Latitude", None),
            geo_longitude=geo.get("Longitude", None),
            geo_city=geo.get("USPS Preferred City Name", None),
            geo_xy_coord=geo.get("Spatial X-Y Coordinates of Address", None),
            geo_commboard=geo.get("COMMUNITY DISTRICT", {}).get(
                "COMMUNITY DISTRICT", None
            ),
            geo_nta2010=geo.get("Neighborhood Tabulation Area (NTA)", None),
            geo_nta2020=geo.get("2020 Neighborhood Tabulation Area (NTA)", None),
            geo_council=geo.get("City Council District", None),
            geo_ct2010=geo.get("2010 Census Tract", None),
            geo_ct2020=geo.get("2020 Census Tract", None),
            geo_grc=geo.get("Geosupport Return Code (GRC)", None),
            geo_grc2=geo.get("Geosupport Return Code 2 (GRC 2)", None),
            geo_reason_code=geo.get("Reason Code", None),
            geo_message=geo.get("Message", "msg err"),
            geo_policeprct=geo.get("Police Precinct", None),
            geo_schooldist=geo.get("Community School District", None),
        )

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            df = func()
            df = self.geocode_a_dataframe(df)
            return df

        return wrapper
