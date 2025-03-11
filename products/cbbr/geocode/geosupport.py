import pandas as pd
import copy
import re
from multiprocessing import Pool, cpu_count
from geosupport import Geosupport, GeosupportError
from .helpers import (
    GEOCODE_COLUMNS,
    get_hnum,
    get_sname,
    geo_parser,
)
from dcpy.utils import postgres

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
    street_name = input_record["site_or_facility_name"] or ""
    street_name = re.sub(r"[^\x00-\x7F]+", "", street_name)

    geo_function_result = geo_client["1B"](
        borough=borough,
        street_name=street_name,
        house_number="",
    )

    return geo_function_result


def geosupport_2(input_record: dict) -> dict:
    """2 function - geocode intersection based on the two street names"""
    borough = input_record.get("borough_code")
    street_1 = input_record.get("intersection_street_1")
    street_2 = input_record.get("intersection_street_2")

    # use geosupport function
    geo_function_result = geo_client["2"](
        borough=borough,
        street_name_1=street_1,
        street_name_2=street_2,
    )

    return geo_function_result


def geosupport_3(input_record: dict) -> dict:
    """3 function - geocode street segment (1 block) based on the three street names"""
    borough = input_record.get("borough_code")
    street_name = input_record.get("street_name")
    cross_street_1 = input_record.get("cross_street_1")
    cross_street_2 = input_record.get("cross_street_2")
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
    geosupport_2,
    # geosupport_2_cross_streets,
    geosupport_3,
]


def geocode_record(inputs: dict) -> dict:
    outputs = copy.deepcopy(inputs)

    if inputs["location_specific"] != "Yes":
        outputs.update(dict(geo_function="NOT LOCATION DATA"))
        return outputs

    for geo_function in GEOSUPPORT_FUNCTION_HIERARCHY:
        last_attempted_geo_function = geo_function.__name__
        try:
            geo_result = geo_function(input_record=inputs)
            geo_result = geo_parser(geo_result)
            outputs.update(dict(geo_function=last_attempted_geo_function))
            outputs.update(geo_result)

            return outputs

        except GeosupportError:
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
    outputs.update(dict(geo_function="GEOCODING FAILED"))

    return outputs


def geocode_records(cbbr_data: pd.DataFrame) -> pd.DataFrame:
    cbbr_data[GEOCODE_COLUMNS] = None
    data_records = cbbr_data.to_dict("records")
    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        geocoded_records = pool.map(geocode_record, data_records, 10000)

    return pd.DataFrame(geocoded_records)


if __name__ == "__main__":
    print("start selecting data from DB ...")
    # connect to postgres db
    client = postgres.PostgresClient()
    cbbr_data = client.read_table_df("_cbbr_submissions")

    print("parsing location data for geocoding ...")
    cbbr_data = cbbr_data.where(pd.notnull(cbbr_data), None)
    cbbr_data["addressnum"] = cbbr_data["address"].apply(get_hnum)
    cbbr_data["street_name"] = cbbr_data["address"].apply(get_sname)

    client.insert_dataframe(cbbr_data, "_cbbr_submissions_address_parsed")
    print("start geocoding ...")
    geocoded_cbbr_data = geocode_records(cbbr_data)
    print("start exporting to DB ...")
    client.insert_dataframe(
        geocoded_cbbr_data, "_cbbr_submissions", if_exists="replace"
    )
    print("done geocoding")
