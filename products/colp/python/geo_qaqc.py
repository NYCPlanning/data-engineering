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
        hnum=geo.get("House Number - Display Format", ""),
        sname=geo.get("First Street Name Normalized", ""),
        input_bbl=geo.get("input_bbl", ""),
        input_hnum=geo.get("input_hnum", ""),
        input_sname=geo.get("input_sname", ""),
        grc_1e=geo.get("Geosupport Return Code (GRC)", ""),
        grc_1a=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        warn_1e=geo.get("Warning Code", ""),
        warn_1a=geo.get("Warning Code 2", ""),
        rsn_1e=geo.get("Reason Code", ""),
        rsn_1a=geo.get("Reason Code 2", ""),
        msg_1e=geo.get("Message", ""),
        msg_1a=geo.get("Message 2", ""),
    )


def geocode(hnum_in, sname_in, borough_in):
    try:
        geo = g["1B"](
            street_name=sname_in,
            house_number=hnum_in,
            borough=borough_in,
            mode="regular",
        )
        geo = parse_output(geo)
        return geo
    except GeosupportError as e:
        geo = parse_output(e.result)
        return geo


def run_1b(inputs):
    uid = inputs.get("uid")
    house_number = inputs.get("house_number")
    street_name = inputs.get("street_name")
    bbl = inputs.get("bbl")
    geo_dcas = geocode(house_number, street_name, bbl[0])
    return {
        "uid": uid,
        "dcas_bbl": bbl,
        "dcas_hnum": house_number,
        "dcas_sname": street_name,
        "hnum_1b": geo_dcas.get("hnum", ""),
        "sname_1b": geo_dcas.get("sname", ""),
        "bbl_1b": geo_dcas.get("geo_bbl", ""),
        "bill_bbl_1b": geo_dcas.get("bill_bbl", ""),
        "grc_1e": geo_dcas.get("grc_1e", ""),
        "rsn_1e": geo_dcas.get("rsn_1e", ""),
        "warn_1e": geo_dcas.get("warn_1e", ""),
        "msg_1e": geo_dcas.get("msg_1e", ""),
        "grc_1a": geo_dcas.get("grc_1a", ""),
        "rsn_1a": geo_dcas.get("rsn_1a", ""),
        "warn_1a": geo_dcas.get("warn_1a", ""),
        "msg_1a": geo_dcas.get("msg_1a", ""),
    }


if __name__ == "__main__":
    with engine.begin() as conn:
        df = pd.read_sql(
            text(
                """
            SELECT DISTINCT 
                uid, 
                ipis_bbl as bbl, 
                house_number, 
                street_name
            FROM geo_inputs"""
            ),
            conn,
        )

        df.house_number = df.house_number.str.rstrip(r"¦")
        print(f"Input data shape: {df.shape}")

        records = df.to_dict("records")

        # Multiprocess
        print("Geocode DCAS addresses with 1B...")
        with Pool(processes=cpu_count()) as pool:
            it = pool.map(run_1b, records, 10000)

        print("Geocoding finished ...")
        result = pd.DataFrame(it)
        print(result.head())

        result.to_sql(
            "geo_qaqc",
            con=conn,
            if_exists="replace",
            index=False,
            method=psql_insert_copy,
        )
