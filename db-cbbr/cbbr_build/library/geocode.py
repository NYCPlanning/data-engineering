import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import copy
from multiprocessing import Pool, cpu_count
from geosupport import Geosupport, GeosupportError
from library.helper.geocode_utils import (
    GEOCODE_COLUMNS,
    LOCATION_PREFIX_TO_COLUMN,
    parse_location,
    get_hnum,
    get_sname,
    geo_parser,
)

geo_client = Geosupport()


def geosupport_1B_address(input_record: dict) -> dict:
    """1B function - geocode address based on the address number and street name"""
    borough = input_record.get("borough_code")
    house_number = input_record.get("addressnum")
    street_name = input_record.get("street_name")
    if not house_number:
        # no house_number indicates unlikely to be an address
        # NOTE This error message won't show in data since, if no function work, geo_message will be GEOCODING FAILED
        raise GeosupportError("UNLIKELY TO BE AN ADDRESS")

    # use geosupport function
    geo_function_result = geo_client["1B"](
        borough=borough,
        street_name=street_name,
        house_number=house_number,
    )

    return geo_function_result


def geosupport_1B_place(input_record: dict) -> dict:
    """1B function - geocode address based on the place name"""
    borough = input_record.get("borough_code")
    street_name = input_record.get("facility_or_park_name")

    # use geosupport function
    geo_function_result = geo_client["1A"](
        borough=borough,
        street_name=street_name,
        house_number="",
    )

    return geo_function_result


def geosupport_2_street_name(input_record: dict) -> dict:
    """2 function - geocode intersection based on the two street names (primary and 1st cross street)"""
    borough = input_record.get("borough_code")
    street_name = input_record.get("street_name")
    cross_street_1 = input_record.get("between_cross_street_1")
    cross_street_2 = input_record.get("and_cross_street_2")
    if cross_street_2 and cross_street_1 != cross_street_2:
        # non-null value for cross_street_2 that isn't the same as cross_street_1
        # indicates a likely street segment
        # NOTE This error message won't show in data since, if no function work, geo_message will be GEOCODING FAILED
        raise GeosupportError("UNLIKELY TO BE AN INTERSECTION")

    # use geosupport function
    geo_function_result = geo_client["2"](
        borough=borough,
        street_name_1=street_name,
        street_name_2=cross_street_1,
    )

    return geo_function_result


def geosupport_3(input_record: dict) -> dict:
    """3 function - geocode street segment (1 block) based on the three street names"""
    borough = input_record.get("borough_code")
    street_name = input_record.get("street_name")
    cross_street_1 = input_record.get("between_cross_street_1")
    cross_street_2 = input_record.get("and_cross_street_2")
    if not cross_street_1 or not cross_street_2:
        raise GeosupportError("UNLIKELY TO BE A STREET SEGMENT")

    # use geosupport function
    geo_function_result = geo_client["3"](
        borough=borough,
        street_name_1=street_name,
        street_name_2=cross_street_1,
        street_name_3=cross_street_2,
    )
    # determine other geography details
    geo_from_node = geo_function_result.get("From Node", None)
    geo_to_node = geo_function_result.get("To Node", None)
    geo_from_coords = geo_client["2"](node=geo_from_node).get("SPATIAL COORDINATES", {})

    geo_function_result["geo_from_x_coord"] = geo_from_coords.get("X Coordinate", "")
    geo_function_result["geo_from_y_coord"] = geo_from_coords.get("Y Coordinate", "")

    geo_from_coords = geo_client["2"](node=geo_to_node).get("SPATIAL COORDINATES", {})
    geo_function_result["geo_to_x_coord"] = geo_from_coords.get("X Coordinate", "")
    geo_function_result["geo_to_y_coord"] = geo_from_coords.get("Y Coordinate", "")

    return geo_function_result


GEOSUPPORT_FUNCTION_HIERARCHY = [
    geosupport_1B_address,
    geosupport_1B_place,
    geosupport_2_street_name,
    # geosupport_2_cross_streets,
    geosupport_3,
]


def geocode_record(inputs: dict) -> dict:
    geo_error = None
    outputs = copy.deepcopy(inputs)

    input_location = inputs.get("location")
    if not input_location:
        outputs.update(dict(geo_function="NO LOCATION DATA"))
        return outputs

    if not any([pair[0] in input_location for pair in LOCATION_PREFIX_TO_COLUMN]):
        outputs.update(dict(geo_function="INVALID LOCATION DATA"))
        return outputs

    if "district wide" in input_location.lower():
        outputs.update(dict(geo_function="LOCATION IS DISTRICT WIDE"))
        return outputs

    for geo_function in GEOSUPPORT_FUNCTION_HIERARCHY:
        last_attempted_geo_function = geo_function.__name__
        try:
            geo_result = geo_function(input_record=inputs)
            geo_result = geo_parser(geo_result)
            outputs.update(dict(geo_function=last_attempted_geo_function))
            outputs.update(geo_result)

            return outputs

        except GeosupportError as e:
            # # try the next function but remember this error
            # geo_error = e
            continue

    # geo_result_error = geo_error.result
    # geo_result_error = geo_parser(geo_result_error)
    # outputs.update(dict(geo_function=f"{last_attempted_geo_function} FAILED"))
    # outputs.update(geo_result_error)

    # TODO for a record, store all errors as each function is attempted
    # maybe format as geosupport_1B_address: Error message; geosupport_1B_place: Error message;
    # shorter function names would help
    outputs.update(dict(geo_function=f"GEOCODING FAILED"))

    return outputs


def geocode_records(cbbr_data: pd.DataFrame) -> pd.DataFrame:
    cbbr_data["addressnum"] = cbbr_data["address"].apply(get_hnum)
    cbbr_data["street_name"] = cbbr_data["address"].apply(get_sname)
    cbbr_data[GEOCODE_COLUMNS] = None

    data_records = cbbr_data.to_dict("records")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        geocoded_records = pool.map(geocode_record, data_records, 10000)

    return pd.DataFrame(geocoded_records)


if __name__ == "__main__":
    print("start selecting data from DB ...")
    # connect to postgres db
    engine = create_engine(os.environ["BUILD_ENGINE"])
    select_query = text("SELECT * FROM _cbbr_submissions")
    with engine.begin() as conn:
        cbbr_data = pd.read_sql(select_query, conn)

    print("parsing location data for geocoding ...")
    cbbr_data = parse_location(cbbr_data)

    print("start geocoding ...")
    geocoded_cbbr_data = geocode_records(cbbr_data)
    print("start exporting to DB ...")
    geocoded_cbbr_data.to_sql(
        "_cbbr_submissions", engine, if_exists="replace", chunksize=500, index=False
    )
    print("done geocoding")
