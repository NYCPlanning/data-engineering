from multiprocessing import Pool, cpu_count

import pandas as pd
from geosupport import Geosupport, GeosupportError
from sqlalchemy import text

from .utils import engine, psql_insert_copy

g = Geosupport()


def parse_output(geo):
    return dict(
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)", ""
        ),
        bill_bbl=geo.get("Condominium Billing BBL", ""),
        latitude=geo.get("Latitude", ""),
        longitude=geo.get("Longitude", ""),
        x_coord="",
        y_coord="",
        input_bbl=geo.get("input_bbl", ""),
        grc=geo.get("Geosupport Return Code (GRC)", ""),
        rsn=geo.get("Reason Code", ""),
        msg=geo.get("Message", ""),
    )


def geocode(inputs):
    bbl = inputs.pop("bbl")

    # Run input BBL through BL to get address. BL output gets saved in case 1A/1B fail.
    try:
        geo_bl = g["BL"](bbl=bbl)
        geo_bl = parse_output(geo_bl)

    except GeosupportError as e1:
        geo_bl = parse_output(e1.result)

    geo_bl.update(input_bbl=bbl, geo_function="BL")
    return geo_bl


if __name__ == "__main__":
    with engine.begin() as conn:
        df = pd.read_sql(
            text(
                """
                            SELECT DISTINCT geo_bbl as bbl
                            FROM geo_inputs
                        """
            ),
            conn,
        )
        print(f"input data shape: {df.shape}")

        records = df.to_dict("records")

        print("geocoding begins here ...")
        # Multiprocess
        with Pool(processes=cpu_count()) as pool:
            it = pool.map(geocode, records, 10000)

        print("geocoding finished ...")
        result = pd.DataFrame(it)
        print(result.head())

        result.to_sql(
            "dcas_ipis_geocodes",
            con=conn,
            if_exists="replace",
            index=False,
            method=psql_insert_copy,
        )

        conn.execute(
            text(
                """
            ALTER TABLE dcas_ipis_geocodes
                ADD wkb_geometry geometry(Geometry,4326);
            UPDATE dcas_ipis_geocodes
            SET wkb_geometry = ST_SetSRID(ST_Point(longitude::DOUBLE PRECISION,
                                latitude::DOUBLE PRECISION), 4326); 
            UPDATE dcas_ipis_geocodes
            SET x_coord = ST_X(ST_TRANSFORM(wkb_geometry, 2263))::text,
                y_coord = ST_Y(ST_TRANSFORM(wkb_geometry, 2263))::text;
        """
            )
        )
