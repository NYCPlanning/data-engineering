from geosupport import Geosupport, GeosupportError
import usaddress
import re

g = Geosupport()


def convert_to_sname(b7sc):
    """
    Uses the geosupport DG function to convert
    a 7-digit street code to a street name

    Parameters
    ----------
    b7sc: str
        Numeric 7-digit street code

    Returns
    -------
    str
        First street name associated with b7sc,
        in normalized form

    """
    try:
        geo = g["DG"](B7SC=b7sc)
        return geo.get("First Street Name Normalized", "")
    except:
        return ""


def get_borocode(x):
    """
    Convert boro strings into single character
    code
    """
    if (x == "") | (x == None):
        return x
    return x[0]


def get_hnum(address):
    """
    Cleans input address and uses usaddress to create a string
    of house number information

    Parameters
    ----------
    address: str
        String containing address in raw form.
        May contain characters not recognized by geosupport,
        including fractional house numbers.

    Returns
    -------
    result: str
        Address string containing all words labelled as relating to house
        number
    """
    address = "" if address is None else address
    result = [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
    result = " ".join(result)
    fraction = re.findall("\d+[\/]\d+", address)
    if not bool(re.search("\d+[\/]\d+", result)) and len(fraction) != 0:
        result = f"{result} {fraction[0]}"
    return result


def get_sname(address):
    """
    Cleans input address and uses usaddress to create a string
    of street name information

    Parameters
    ----------
    address: str
        String containing address in raw form.
        May contain characters not recognized by geosupport.

    Returns
    -------
    result: str
        Address string containing all words labelled as relating to
        street name
    """
    result = (
        [k for (k, v) in usaddress.parse(address) if re.search("Street", v)]
        if address is not None
        else ""
    )
    result = " ".join(result)
    if result == "":
        return address
    else:
        return result


def geo_parser(geo):
    """
    Parses nested dictionary output by geosupport to extract coordinate
    information, BBL, BIN, and first-level error codes/messages

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's 1B or BL function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed geographic information from geosupport
           results
    """
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
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
        geo_grc2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        geo_reason_code=geo.get("Reason Code", ""),
        geo_message=geo.get("Message", "msg err"),
    )


def geo_parser_bn(geo):
    """
    Parses nested dictionary output by geosupport BN to extract
    all error codes/messages as well as TPAD-related flags

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's BN function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed error, warning, and TPAD info
    """
    return dict(
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)", ""
        ),
        geo_bin=geo.get("Building Identification Number (BIN)", ""),
        geo_tpad_new_bin=geo.get("TPAD New BIN", ""),
        geo_tpad_new_bin_status=geo.get("TPAD New BIN Status", ""),
        geo_tpad_dm_bin_status=geo.get("TPAD BIN Status (for DM job)", ""),
        geo_tpad_conflict_flag=geo.get("TPAD Conflict Flag", ""),
        geo_tpad_bin_status=geo.get("TPAD BIN Status", ""),
        geo_return_code=geo.get("Geosupport Return Code (GRC)", ""),
        geo_return_code_2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        geo_message=geo.get("Message", ""),
        geo_message_2=geo.get("Message 2", "msg2 err"),
        geo_reason_code=geo.get("Reason Code", ""),
    )


def geo_parser_1(geo):
    """
    Parses nested dictionary output by geosupport 1B to extract
    all error codes/messages as well as TPAD-related flags

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's 1* function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed error, warning, and TPAD info
    """
    return dict(
        geo_hnum=geo.get("House Number - Display Format", ""),
        geo_sname=geo.get("First Street Name Normalized", ""),
        geo_borough=geo.get("First Borough Name", ""),
        geo_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)", ""
        ),
        geo_bin=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", ""
        ),
        geo_tpad_new_bin=geo.get("TPAD New BIN", ""),
        geo_tpad_new_bin_status=geo.get("TPAD New BIN Status", ""),
        geo_tpad_dm_bin_status=geo.get("TPAD BIN Status (for DM job)", ""),
        geo_tpad_conflict_flag=geo.get("TPAD Conflict Flag", ""),
        geo_tpad_bin_status=geo.get("TPAD BIN Status", ""),
        geo_return_code=geo.get("Geosupport Return Code (GRC)", ""),
        geo_return_code_2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        geo_message=geo.get("Message", ""),
        geo_message_2=geo.get("Message 2", "msg2 err"),
        geo_reason_code=geo.get("Reason Code", ""),
    )


def geo_parser_1a(geo):
    """
    Parses nested dictionary output by geosupport 1A to extract
    spatial info as well as error codes/messages

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's 1A function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed error, warning, and spatial info
    """
    return dict(
        geo_housenum=geo.get("House Number - Display Format", ""),
        geo_streetname=geo.get("First Street Name Normalized", ""),
        geo_b10sc=geo.get("B10SC - First Borough and Street Code", ""),
        geo_return_code=geo.get("Geosupport Return Code (GRC)", ""),
        geo_message=geo.get("Message", ""),
        geo_reason_code=geo.get("Reason Code", ""),
    )


def geo_parser_1b(geo):
    """
    Parses nested dictionary output by geosupport 1B to extract
    spatial info as well as error codes/messages

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's 1A function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed error, warning, and spatial info
    """
    return dict(
        geo_atomicpolygon=geo.get("Atomic Polygon", ""),
        geo_housenum=geo.get("House Number - Display Format", ""),
        geo_streetname=geo.get("First Street Name Normalized", ""),
        geo_b10sc=geo.get("B10SC - First Borough and Street Code", ""),
        geo_censtract=geo.get("2020 Census Tract", ""),
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
        geo_reason_code=geo.get("Reason Code", ""),
        geo_message=geo.get("Message", "msg err"),
    )


def geo_parser_1e(geo):
    """
    Parses nested dictionary output by geosupport 1E to extract
    spatial info as well as error codes/messages

    Parameters
    ----------
    geo: dict of dicts
            Results returned from python-geosupport's 1E function,
            with the addition of input address/BBL fields
    Returns
    -------
    geo : dict
           Dictionary containing parsed error, warning, and spatial info
    """
    return dict(
        geo_atomicpolygon=geo.get("Atomic Polygon", ""),
        geo_housenum=geo.get("House Number - Display Format", ""),
        geo_streetname=geo.get("First Street Name Normalized", ""),
        geo_b10sc=geo.get("B10SC - First Borough and Street Code", ""),
        geo_censtract=geo.get("2010 Census Tract", ""),
        geo_grc=geo.get("Geosupport Return Code (GRC)", ""),
        geo_reason_code=geo.get("Reason Code", ""),
        geo_message=geo.get("Message", "msg err"),
    )


def geo_parser_1n(geo):
    return dict(
        geo_reason_code=geo.get("Reason Code", ""),
        geo_return_code=geo.get("Geosupport Return Code (GRC)", ""),
        geo_message=geo.get("Message", ""),
    )
