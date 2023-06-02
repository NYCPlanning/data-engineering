import re
import pandas as pd
import numpy as np
import usaddress
from typing import Union

# location components ordered by the observed pattern in data from right to left
# each component is paired with the column to store its value in for geocoding
# Site Name: xxx Street Name: xxx Cross Street 1 xxx Cross Street 2
LOCATION_PREFIX_TO_COLUMN = [
    ("Cross Street 2:", "and_cross_street_2"),
    ("Cross Street 1:", "between_cross_street_1"),
    ("Street Name:", "address"),
    ("Site Name:", "facility_or_park_name"),
]

GEOCODE_COLUMNS = [
    "geo_function",
    "geo_message",
    "geo_grc",
    "geo_reason_code",
    "geo_housenum",
    "geo_streetname",
    "geo_bbl",
    "geo_borough",
    "geo_bin",
    "geo_latitude",
    "geo_longitude",
    "geo_x_coord",
    "geo_y_coord",
    "geo_from_node",
    "geo_from_x_coord",
    "geo_from_y_coord",
    "geo_to_node",
    "geo_to_x_coord",
    "geo_to_y_coord",
]


def get_location_value_from_end(
    full_location_value: str, location_value_prefix: str
) -> Union[str, float]:
    """Extract a prefixed location value from the end of a location string.
    Will return all text to the right of the prefix.

    Examples
        'Site Name: A', 'Site Name:' -> 'A'
        'Site Name: A;', 'Site Name:' -> 'A'
        'Site Name: A Cross Street: B', 'Cross Street:' -> 'B'
        'Site Name: A Cross Street: B', 'Site Name:' -> 'A Cross Street: B'
    """
    if pd.isna(full_location_value):
        return None
    if location_value_prefix not in full_location_value:
        return None

    all_substrings = full_location_value.split(location_value_prefix)

    invalid_substrings = ["", " "]
    invalid_characters = [";"]
    valid_substrings = [
        substring.strip()
        for substring in all_substrings
        if substring not in invalid_substrings
    ]
    location_value = valid_substrings[-1]

    for invalid_character in invalid_characters:
        location_value = location_value.replace(invalid_character, "")

    return location_value


def remove_location_value_from_end(
    full_location_value: str, location_value_prefix: str
) -> Union[str, float]:
    """Remove a prefixed location value from the end of a location string.
    Will return all text to the right of the prefix.

    Examples
        'Site Name: A', 'Site Name:' -> None
        'Site Name: A Cross Street: B', 'Cross Street:' -> 'Site Name: A'
        'Site Name: A Cross Street: B', 'Site Name:' -> None
    """
    if pd.isna(full_location_value):
        return None
    if location_value_prefix not in full_location_value:
        return full_location_value.strip()

    return full_location_value.split(location_value_prefix)[0].strip()


def parse_location(data: pd.DataFrame) -> pd.DataFrame:
    """Parses address data in a location column.

    Splits substrings from the location column into separate columns for geocoding
    via Geosupport.

    Args:
        data:
            A dataframe with a location column

    Returns:
        A dataframe with additional columns for each component
    """
    temp_location_column = "location_parsing"
    data[temp_location_column] = data["location"]
    for location_prefix_column_pair in LOCATION_PREFIX_TO_COLUMN:
        # get prefix and column name for this pair
        location_component_prefix = location_prefix_column_pair[0]
        new_column = location_prefix_column_pair[1]
        # get the location component
        data[new_column] = data[temp_location_column].apply(
            lambda x: get_location_value_from_end(x, location_component_prefix)
        )
        # remove the location prefix and component
        data[temp_location_column] = data[temp_location_column].apply(
            lambda x: remove_location_value_from_end(x, location_component_prefix)
        )

    data = data.drop([temp_location_column], axis=1)
    # convert all nan values to None
    data = data.where(pd.notnull(data), None)

    return data


def get_hnum(address: str) -> str:
    if not address:
        return None
    result = [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
    result = " ".join(result)
    fraction = re.findall("\d+[\/]\d+", address)
    if not bool(re.search("\d+[\/]\d+", result)) and len(fraction) != 0:
        result = f"{result} {fraction[0]}"

    if result == "":
        return None

    return result


def get_sname(address: str) -> str:
    if not address:
        return None
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


def clean_streetname(address: str, n: int) -> str:
    address = "" if address is None else address
    if (" at " in address.lower()) | (" and " in address.lower()) | ("&" in address):
        address = re.split("&| AND | and | AT | at", address)[n]
    else:
        address = ""
    return address


def geo_parser(geo: dict) -> dict:
    parsed_geo = dict(
        geo_message=geo.get("Message", None),
        geo_grc=geo.get("Geosupport Return Code (GRC)", None),
        geo_reason_code=geo.get("Reason Code", None),
        geo_housenum=geo.get("House Number - Display Format", None),
        geo_streetname=geo.get("First Street Name Normalized", None),
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)",
            None,
        ),
        geo_borough=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "Borough Code",
            None,
        ),
        geo_bin=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", None
        ),
        geo_latitude=geo.get("Latitude", None),
        geo_longitude=geo.get("Longitude", None),
        geo_x_coord=geo.get("SPATIAL COORDINATES", {}).get("X Coordinate", None),
        geo_y_coord=geo.get("SPATIAL COORDINATES", {}).get("Y Coordinate", None),
        geo_from_node=geo.get("From Node", None),
        geo_from_x_coord=geo.get("geo_from_x_coord", None),
        geo_from_y_coord=geo.get("geo_from_y_coord", None),
        geo_to_node=geo.get("To Node", None),
        geo_to_x_coord=geo.get("geo_to_x_coord", None),
        geo_to_y_coord=geo.get("geo_to_y_coord", None),
        # geo_grc2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        # geo_message2=geo.get("Message 2", "msg2 err"),
    )
    # replace empty strings with None
    for column in parsed_geo:
        if parsed_geo.get(column) == "":
            parsed_geo[column] = None

    return parsed_geo
