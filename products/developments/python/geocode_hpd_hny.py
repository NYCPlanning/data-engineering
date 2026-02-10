import os
from multiprocessing import Pool, cpu_count

import pandas as pd
from geosupport import Geosupport, GeosupportError
from sqlalchemy import create_engine, text
from utils import psql_insert_copy

g = Geosupport()


def geocode(input):
    # collect inputs
    uid = str(input.pop("ogc_fid"))
    hnum = input.pop("number")
    sname = input.pop("street")
    borough = input.pop("borough")

    try:
        geo = g["1B"](
            street_name=sname,
            house_number=hnum,
            borough=borough,
            mode="regular",
        )
        geo = parse_output(geo)
        geo.update(
            dict(
                uid=uid,
                mode="regular",
                func="1B",
                status="success",
            )
        )
    except GeosupportError:
        try:
            geo = g["1B"](
                street_name=sname,
                house_number=hnum,
                borough=borough,
                mode="tpad",
            )
            geo = parse_output(geo)
            geo.update(
                dict(
                    uid=uid,
                    mode="tpad",
                    func="1B",
                    status="success",
                )
            )
        except GeosupportError as e:
            geo = parse_output(e.result)
            geo.update(
                uid=uid,
                mode="tpad",
                func="1B",
                status="failure",
            )
    return geo


def parse_output(geo):
    return dict(
        # Normalized address:
        geo_sname=geo.get("First Street Name Normalized", ""),
        geo_hnum=geo.get("House Number - Display Format", ""),
        # Longitude and latitude of lot center
        geo_latitude=geo.get("Latitude", ""),
        geo_longitude=geo.get("Longitude", ""),
        # Some sample administrative areas:
        geo_bin=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", ""
        ),
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)",
            "",
        ),
    )


if __name__ == "__main__":
    # connect to postgres db
    engine = create_engine(os.environ["BUILD_ENGINE"])

    # read in housing table
    select_query = """
            SELECT * 
            FROM hpd_hny_units_by_building
            WHERE reporting_construction_type = 'New Construction'
            AND project_name <> 'CONFIDENTIAL';
            """
    with engine.begin() as conn:
        df = pd.read_sql(
            text(select_query),
            conn,
        )

    df = df.rename(
        columns={
            "latitude_(internal)": "latitude_internal",
            "longitude_(internal)": "longitude_internal",
        }
    )

    records = df.to_dict("records")

    print("Geocoding HNY...")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    print("Geocoding finished, dumping to postgres ...")
    df = pd.DataFrame(it)
    df.to_sql(
        "hny_geocode_results",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
