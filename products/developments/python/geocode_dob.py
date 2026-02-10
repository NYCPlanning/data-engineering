import os
from multiprocessing import Pool, cpu_count

import pandas as pd
from geosupport import Geosupport, GeosupportError
from sqlalchemy import create_engine, text
from utils import psql_insert_copy

g = Geosupport()


def geocode(input):
    try:
        geo = g["1B"](
            street_name=input["address_street"],
            house_number=input["address_numbr"],
            borough=input["boro"],
            mode="regular",
        )
        geo = parse_output(geo)
        geo.update(
            dict(
                id=input["id"],
                mode="regular",
                func="1B",
                status="success",
            )
        )
    except GeosupportError:
        try:
            geo = g["1B"](
                street_name=input["address_street"],
                house_number=input["address_numbr"],
                borough=input["boro"],
                mode="tpad",
            )
            geo = parse_output(geo)
            geo.update(
                dict(
                    id=input["id"],
                    mode="tpad",
                    func="1B",
                    status="success",
                )
            )
        except GeosupportError as e:
            geo = parse_output(e.result)
            geo.update(
                id=input["id"],
                mode="tpad",
                func="1B",
                status="failure",
            )

    geo.update(input)
    return geo


def parse_output(geo):
    return dict(
        # Normalized address:
        geo_address_street=geo.get("First Street Name Normalized", ""),
        geo_address_numbr=geo.get("House Number - Display Format", ""),
        # longitude and latitude of lot center
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
        geo_boro=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "Borough Code",
            "",
        ),
        geo_cd=geo.get("COMMUNITY DISTRICT", {}).get("COMMUNITY DISTRICT", ""),
        geo_firedivision=geo.get("Fire Division", ""),
        geo_firebattalion=geo.get("Fire Battalion", ""),
        geo_firecompany=geo.get("Fire Company Type", "")
        + geo.get("Fire Company Number", ""),
        geo_council=geo.get("City Council District", ""),
        geo_csd=geo.get("Community School District", ""),
        geo_policeprct=geo.get("Police Precinct", ""),
        geo_zipcode=geo.get("ZIP Code", ""),
        geo_nta2010=geo.get("Neighborhood Tabulation Area (NTA)", None),
        geo_nta2020=geo.get("2020 Neighborhood Tabulation Area (NTA)", None),
        geo_ct2010=geo.get("2010 Census Tract", None),
        geo_ct2020=geo.get("2020 Census Tract", None),
        geo_cb2010=geo.get("2010 Census Block", None),
        geo_cb2020=geo.get("2020 Census Block", None),
        geo_cdta2020=geo.get("2020 Community District Tabulation Area (CDTA)", None),
        # the return codes and messaged are for diagnostic puposes
        grc=geo.get("Geosupport Return Code (GRC)", ""),
        grc2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        msg=geo.get("Message", "msg err"),
        msg2=geo.get("Message 2", "msg2 err"),
    )


if __name__ == "__main__":
    # connect to BUILD_ENGINE
    engine = create_engine(os.environ["BUILD_ENGINE"])

    # select records to be geocoded
    # NOTE: using the Group ID (gid) value to limit selection to
    #       the most recent version of duplicate records
    select_query = """
            SELECT 
                id, 
                address_numbr,
                address_street,
                boro,
                datasource
            FROM _init_devdb
            """
    with engine.begin() as conn:
        df = pd.read_sql(
            text(select_query),
            conn,
        )

    records = df.to_dict("records")
    del df

    print("geocoding begins here ...")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    print("geocoding finished, dumping GEO_devdb postgres ...")
    df = pd.DataFrame(it)
    df.to_sql(
        "_geo_devdb",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy,
    )
