import re
import usaddress


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
