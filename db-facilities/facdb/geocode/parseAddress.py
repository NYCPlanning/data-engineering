import re
from functools import wraps

import usaddress


def get_hnum(address):
    address = "" if address is None else quick_clean(address)
    result = (
        [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
        if address is not None
        else ""
    )
    result = " ".join(result)
    if result == "":
        return address
    else:
        return result


def get_sname(address):
    address = "" if address is None else quick_clean(address)
    result = (
        [k for (k, v) in usaddress.parse(address) if re.search("Street", v)]
        if address is not None
        else ""
    )
    result = " ".join(result)
    return result


def quick_clean(address):
    address = (
        "-".join([i.strip() for i in address.split("-")]) if address is not None else ""
    )
    result = [
        k
        for (k, v) in usaddress.parse(address)
        if not v in ["OccupancyIdentifier", "OccupancyType"]
    ]
    return re.sub(r"[,\%\$\#\@\!\_\.\?\`\"\(\)\ï\¿\½\�]", "", " ".join(result))


def ParseAddress(function: callable = None, raw_address_field: str = None):
    def _ParseAddress(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            df = func()
            df["raw_address"] = df[raw_address_field]
            df["cleaned_address"] = df.raw_address.apply(quick_clean)
            df["parsed_hnum"] = df.cleaned_address.apply(get_hnum)
            df["parsed_sname"] = df.cleaned_address.apply(get_sname)
            return df

        return wrapper

    if function:
        return _ParseAddress(function)
    return _ParseAddress


def UseAirportName(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        df = func()

        df["parsed_sname"] = df.apply(axis=1, func=find_sname)
        return df

    def find_sname(row):

        if row["parsed_sname"] != "":
            return row["parsed_sname"]
        if row["airport_name"] != "":
            return row["airport_name"]
        return row["manager_address"]

    return wrapper
