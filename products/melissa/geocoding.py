"""
Geosupport calling logic, ported from db-melissa's python/geocoding.py.

Calls Geosupport Functions 1E (extended), 1A (tpad+extended), and AP (extended)
for a single address, and flattens the responses into the field set that
pipeline.py's DuckDB fill step expects (mirroring db-melissa's sql/fill.sql,
which reads these same e_/a_/ap_-prefixed keys out of Postgres columns).
"""

import re

import usaddress
from geosupport import Geosupport, GeosupportError

g = Geosupport()


def geocode(record: dict) -> dict:
    address = str(record.get("address") or "")
    zip_code = str(record.get("zip_code") or "")
    boro = str(record.get("boro") or "")
    id = str(record.get("id") or "")

    hnum = get_hnum(address)
    sname = get_sname(address)

    geo1e = parse_1e(geo_try(hnum, sname, zip_code, boro, "1E", "extended"))
    geo1a = parse_1a(geo_try(hnum, sname, zip_code, boro, "1A", "tpad+extended"))
    geoap = parse_ap(geo_try(hnum, sname, zip_code, boro, "AP", "extended"))

    geo = {**geo1a, **geo1e, **geoap}
    geo.update(dict(id=id, hnum=hnum, sname=sname))
    return geo


def geo_try(hnum, sname, zip_code, boro, func, mode):
    try:
        if boro == "":
            return g[func](
                street_name=sname, house_number=hnum, zip_code=zip_code, mode=mode
            )
        else:
            return g[func](
                street_name=sname, house_number=hnum, borough=boro, mode=mode
            )
    except GeosupportError as e:
        return e.result


def get_hnum(address):
    if "|" in address:
        return address.split("|")[0]
    elif "term mkt" in address.lower():
        return address.split(" ")[0]
    else:
        fraction = re.findall(r"\d+[\/]\d+", address)
        rear = re.findall(" rear ", address, re.IGNORECASE)
        result = (
            [k for (k, v) in usaddress.parse(address) if re.search("Address", v)]
            if address is not None
            else ""
        )
        hnum = " ".join(result)
        if bool(re.search(r"\d+[\/]\d+", hnum)) and len(fraction) != 0:
            pass
        else:
            if not bool(re.search(r"\d+[\/]\d+", hnum)) and len(fraction) != 0:
                hnum = f"{hnum} {fraction[0]}"

        if len(rear) != 0:
            hnum = f"{hnum} rear"
        return hnum


def get_sname(address):
    if "|" in address:
        return address.split("|")[1]
    elif "term mkt" in address.lower():
        return " ".join(address.split(" ")[1:])
    else:
        fraction = re.findall(r"\d+[\/]\d+", address)
        rear = re.findall(" rear ", address, re.IGNORECASE)

        result = (
            [k for (k, v) in usaddress.parse(address) if re.search("Street", v)]
            if address is not None
            else ""
        )
        result = " ".join(result)
        if len(fraction) != 0:
            for i in fraction:
                result = result.replace(i, "")
        if len(rear) != 0:
            result = result.lower().replace("rear", "")
        if result == "":
            return address
        else:
            return result


def parse_1e(geo):
    return dict(
        e_wa1_housenumberdisplay=geo.get("House Number - Display Format", ""),
        e_wa1_street1_boroughcode=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "Borough Code",
            "",
        ),
        e_wa1_street1_streetname=geo.get("First Street Name Normalized", ""),
        e_wa1_message=geo.get("Message", "msg err"),
        e_wa2_xcoordinate=geo.get("SPATIAL X-Y COORDINATES OF ADDRESS", {}).get(
            "X Coordinate",
            "",
        ),
        e_wa2_ycoordinate=geo.get("SPATIAL X-Y COORDINATES OF ADDRESS", {}).get(
            "Y Coordinate",
            "",
        ),
        e_wa2_communitydistrict=geo.get("COMMUNITY DISTRICT", {}).get(
            "COMMUNITY DISTRICT", ""
        ),
        e_wa2_zipcode=geo.get("ZIP Code", ""),
        e_wa2_nta=geo.get("Neighborhood Tabulation Area (NTA)", ""),
        e_wa2_physicalid=geo.get("Physical ID", ""),
        # "NTA Name" is the paired name field for the (2010-vintage) "Neighborhood
        # Tabulation Area (NTA)" code above -- verbatim from db-melissa, but this
        # Geosupport version returns it empty for every address. There's no
        # equivalent paired name field for the 2020-vintage code, so nta_name is
        # instead filled in later (see pipeline.py's fill_nta_names) via a join
        # against DCP's own dcp_nta2020 reference dataset, keyed by
        # e_wa2_nta2020 below.
        e_wa2_ntaname=geo.get("NTA Name", ""),
        e_wa2_nta2020=geo.get("2020 Neighborhood Tabulation Area (NTA)", ""),
        e_wa2_latitude=geo.get("Latitude", ""),
        e_wa2_longitude=geo.get("Longitude", ""),
        e_wa2_blockfaceid=geo.get("Blockface ID", ""),
        e_wa2_reasoncode=geo.get("Reason Code", ""),
        e_wa2_grc=geo.get("Geosupport Return Code (GRC)", ""),
    )


def parse_1a(geo):
    return dict(
        a_wa1_housenumberdisplay=geo.get("House Number - Display Format", ""),
        a_wa1_street1_streetname=geo.get("First Street Name Normalized", ""),
        a_wa1_message=geo.get("Message", "msg err"),
        a_wa2_bbl=geo.get("BOROUGH BLOCK LOT (BBL)", {}).get(
            "BOROUGH BLOCK LOT (BBL)",
            "",
        ),
        a_wa2_binofinputaddress=geo.get(
            "Building Identification Number (BIN) of Input Address or NAP", ""
        ),
        a_wa2_tpadnewbin=geo.get("TPAD New BIN", ""),
        a_wa2_reasoncode=geo.get("Reason Code", ""),
        a_wa2_grc=geo.get("Geosupport Return Code (GRC)", ""),
    )


def parse_ap(geo):
    xy_coord = geo.get("X-Y Coordinates of Address Point", "")
    return dict(
        ap_wa1_housenumberdisplay=geo.get("House Number - Display Format", ""),
        ap_wa1_street1_streetname=geo.get("First Street Name Normalized", ""),
        ap_wa2_grc=geo.get("Geosupport Return Code (GRC)", ""),
        ap_wa2_reasoncode=geo.get("Reason Code", ""),
        ap_wa1_message=geo.get("Message", "msg err"),
        ap_wa2_latitude=geo.get("Latitude", ""),
        ap_wa2_longitude=geo.get("Longitude", ""),
        ap_wa2_xcoordinate=xy_coord,
        ap_wa2_ycoordinate=xy_coord,
        ap_wa2_ap_id=geo.get("Address Point ID", ""),
    )
