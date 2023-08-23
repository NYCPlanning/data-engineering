# DEPRECATED in favor of geocode.py
from multiprocessing import Pool, cpu_count
from sqlalchemy import create_engine, text
from geosupport import Geosupport, GeosupportError
from pathlib import Path
import pandas as pd
import usaddress
import json
import re
import os
import numpy as np
from geocode_utils import parse_location

g = Geosupport()


def get_hnum(address):
    address = "" if address is None else address
    result = [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
    result = " ".join(result)
    fraction = re.findall("\d+[\/]\d+", address)
    if not bool(re.search("\d+[\/]\d+", result)) and len(fraction) != 0:
        result = f"{result} {fraction[0]}"
    return result


def get_sname(address):
    address = "" if address is None else address
    result = (
        [k for (k, v) in usaddress.parse(address) if re.search("Street", v)]
        if address is not None
        else ""
    )
    result = " ".join(result)
    if result == "":
        return address.strip(",")
    else:
        return result.strip(",")


def clean_streetname(x, n):
    x = "" if x is None else x
    if (" at " in x.lower()) | (" and " in x.lower()) | ("&" in x):
        x = re.split("&| AND | and | AT | at", x)[n]
    else:
        x = ""
    return x


def geocode(inputs):
    hnum = inputs.get("addressnum", "")
    sname = inputs.get("streetname", "")
    borough = inputs.get("borough", "")
    street_name_1 = inputs.get("streetname_1", "")
    street_name_2 = inputs.get("streetname_2", "")
    sname = inputs.get("streetname", "")
    on_street = inputs.get("street_name", "")
    cross_street_1 = inputs.get("between_cross_street_1", "")
    cross_street_2 = inputs.get("and_cross_street_2", "")

    hnum = str("" if hnum is None else hnum)
    sname = str("" if sname is None else sname)
    borough = str("" if borough is None else borough)
    street_name_1 = str("" if street_name_1 is None else street_name_1)
    street_name_2 = str("" if street_name_2 is None else street_name_2)
    on_street = str("" if on_street is None else on_street)
    cross_street_1 = str("" if cross_street_1 is None else cross_street_1)
    cross_street_2 = str("" if cross_street_2 is None else cross_street_2)

    ## 1B function - geocode basded on the housenum and streetname from address column
    try:
        geo = g["1B"](street_name=sname, house_number=hnum, borough=borough)
        geo = geo_parser(geo)
        geo.update(dict(geo_function="1B"))
    except GeosupportError:
        ## Intersection-1 geocode basded on the streetname_1 and streetname_2 from address or street_name column
        try:
            if (street_name_1 != "") & (street_name_2 != ""):
                geo = g["2"](
                    street_name_1=street_name_1,
                    street_name_2=street_name_2,
                    borough_code=borough,
                )
                geo = geo_parser(geo)
                geo.update(dict(geo_function="Intersection-1"))
            else:
                geo = g["1B"](street_name=sname, house_number=hnum, borough=borough)
                geo = geo_parser(geo)
                geo.update(dict(geo_function="1B"))
        except GeosupportError:
            ## Intersection-2 geocode basded on the street_name and between_cross_street_1 column
            try:
                if (on_street != "") & (cross_street_1 != "") & (cross_street_2 == ""):
                    geo = g["2"](
                        street_name_1=on_street,
                        street_name_2=cross_street_1,
                        borough_code=borough,
                    )
                    geo = geo_parser(geo)
                    geo.update(dict(geo_function="Intersection-2"))
                else:
                    geo = g["1B"](street_name=sname, house_number=hnum, borough=borough)
                    geo = geo_parser(geo)
                    geo.update(dict(geo_function="1B"))
            except GeosupportError:
                ## Intersection-3 geocode basded on street_name and and_cross_street_2 column
                try:
                    if (
                        (on_street != "")
                        & (cross_street_1 == "")
                        & (cross_street_2 != "")
                    ):
                        geo = g["2"](
                            street_name_1=on_street,
                            street_name_2=cross_street_2,
                            borough_code=borough,
                        )
                        geo = geo_parser(geo)
                        geo.update(dict(geo_function="Intersection-3"))
                    else:
                        geo = g["1B"](
                            street_name=sname, house_number=hnum, borough=borough
                        )
                        geo = geo_parser(geo)
                        geo.update(dict(geo_function="1B"))
                except GeosupportError:
                    ## Segment - geocode based on street_name, between_cross_street_1 and and_cross_street_2 column
                    try:
                        if (
                            (on_street != "")
                            & (cross_street_1 != "")
                            & (cross_street_2 != "")
                        ):
                            geo = g["3"](
                                street_name_1=on_street,
                                street_name_2=cross_street_1,
                                street_name_3=cross_street_2,
                                borough_code=borough,
                            )
                            geo = geo_parser(geo)
                            geo_from_node = geo["geo_from_node"]
                            geo_to_node = geo["geo_to_node"]
                            geo_from_x_coord = (
                                g["2"](node=geo_from_node)
                                .get("SPATIAL COORDINATES", {})
                                .get("X Coordinate", "")
                            )
                            geo_from_y_coord = (
                                g["2"](node=geo_from_node)
                                .get("SPATIAL COORDINATES", {})
                                .get("Y Coordinate", "")
                            )
                            geo_to_x_coord = (
                                g["2"](node=geo_to_node)
                                .get("SPATIAL COORDINATES", {})
                                .get("X Coordinate", "")
                            )
                            geo_to_y_coord = (
                                g["2"](node=geo_to_node)
                                .get("SPATIAL COORDINATES", {})
                                .get("Y Coordinate", "")
                            )
                            geo.update(
                                dict(
                                    geo_from_x_coord=geo_from_x_coord,
                                    geo_from_y_coord=geo_from_y_coord,
                                    geo_to_x_coord=geo_to_x_coord,
                                    geo_to_y_coord=geo_to_y_coord,
                                    geo_function="Segment",
                                )
                            )
                        else:
                            geo = g["1B"](
                                street_name=sname, house_number=hnum, borough=borough
                            )
                            geo = geo_parser(geo)
                            geo.update(dict(geo_function="1B"))
                    except GeosupportError:
                        ## Intersection-4 geocode based on street_name, between_cross_street_1 and and_cross_street_2 column
                        try:
                            if (
                                (on_street != "")
                                & (cross_street_1 != "")
                                & (cross_street_2 != "")
                            ):
                                geo1 = g["2"](
                                    street_name_1=on_street,
                                    street_name_2=cross_street_1,
                                    borough_code=borough,
                                )
                                geo2 = g["2"](
                                    street_name_1=on_street,
                                    street_name_2=cross_street_2,
                                    borough_code=borough,
                                )
                                geo1 = geo_parser(geo1)
                                geo2 = geo_parser(geo2)
                                geo_from_x_coord = geo1["geo_x_coord"]
                                geo_from_y_coord = geo1["geo_y_coord"]
                                geo_to_x_coord = geo2["geo_x_coord"]
                                geo_to_y_coord = geo2["geo_y_coord"]
                                geo1.update(
                                    dict(
                                        geo_from_x_coord=geo_from_x_coord,
                                        geo_from_y_coord=geo_from_x_coord,
                                        geo_to_x_coord=geo_from_x_coord,
                                        geo_to_y_coord=geo_from_x_coord,
                                        geo_function="Intersection-4",
                                    )
                                )
                                geo = geo1
                            else:
                                geo = g["1B"](
                                    street_name=sname,
                                    house_number=hnum,
                                    borough=borough,
                                )
                                geo = geo_parser(geo)
                                geo.update(dict(geo_function="1B"))
                        except GeosupportError as e:
                            geo = e.result
                            geo = geo_parser(geo)
                            geo.update(dict(geo_function="GEOCODE FAILED"))

    geo.update(inputs)
    return geo


def geo_parser(geo):
    return dict(
        geo_housenum=geo.get("House Number - Display Format", ""),
        geo_streetname=geo.get("First Street Name Normalized", ""),
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)",
            "",
        ),
        geo_bin=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", ""
        ),
        geo_latitude=geo.get("Latitude", ""),
        geo_longitude=geo.get("Longitude", ""),
        geo_x_coord=geo.get("SPATIAL COORDINATES", {}).get("X Coordinate", ""),
        geo_y_coord=geo.get("SPATIAL COORDINATES", {}).get("Y Coordinate", ""),
        geo_from_node=geo.get("From Node", ""),
        geo_to_node=geo.get("To Node", ""),
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
        geo_grc2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        geo_reason_code=geo.get("Reason Code", ""),
        geo_message=geo.get("Message", "msg err"),
    )


if __name__ == "__main__":
    # connect to postgres db
    engine = create_engine(os.environ["BUILD_ENGINE"])
    select_query = text("SELECT * FROM _cbbr_submissions")
    with engine.begin() as conn:
        df = pd.read_sql(select_query, conn)

    print("parsing data for geocoding ...")
    # parse components of location column
    df = parse_location(df)

    # use parsed location components
    df["addressnum"] = df.address.apply(get_hnum)
    df["streetname"] = df.address.apply(get_sname)
    df["street_name"] = df["streetname"]
    df["streetname_1"] = df["address"].apply(lambda x: clean_streetname(x, 0))
    df["streetname_2"] = df["address"].apply(lambda x: clean_streetname(x, -1))
    df["streetname_1"] = np.where(
        df.streetname_1 == "",
        df.street_name.apply(lambda x: clean_streetname(x, 0)),
        df.streetname_1,
    )
    df["streetname_2"] = np.where(
        df.streetname_2 == "",
        df.street_name.apply(lambda x: clean_streetname(x, 0)),
        df.streetname_2,
    )

    df["geo_from_x_coord"] = np.nan
    df["geo_from_y_coord"] = np.nan
    df["geo_to_x_coord"] = np.nan
    df["geo_to_y_coord"] = np.nan

    # df['between_cross_street_1'] = df['cross_street1'].apply(get_sname)
    # df['and_cross_street_2'] = df['cross_street2'].apply(get_sname)
    df["geo_to_y_coord"] = np.nan
    df["geo_to_y_coord"] = np.nan

    records = df.to_dict("records")

    print("start geocoding ...")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, 10000)

    print("geocoding finished, dumping to postgres ...")
    df = pd.DataFrame(it)
    df.to_sql(
        "_cbbr_submissions", engine, if_exists="replace", chunksize=500, index=False
    )
