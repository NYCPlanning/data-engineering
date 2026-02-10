import os
import re

import pandas as pd
from geosupport import Geosupport, GeosupportError

g = Geosupport()


def geosupport_version() -> str | None:
    geofiles = os.environ.get("GEOFILES")
    if not geofiles:
        return None
    match = re.search(r"version-(\d+[a-zA-Z])", geofiles)
    return match.group(1) if match else None


####### NUMBLDGS ###########################


def get_bbl(inputs):
    bin = inputs["bin"]
    try:
        geo = g["BN"](bin=bin)
    except GeosupportError:
        try:
            geo = g["BN"](bin=bin, mode="tpad")
        except GeosupportError as e2:
            geo = e2.result

    bbl = geo["BOROUGH BLOCK LOT (BBL)"]["BOROUGH BLOCK LOT (BBL)"] or None
    return {"bbl": bbl, "bin": bin}


def geocode_df_bin(df: pd.DataFrame) -> pd.DataFrame:
    print("geocoding begins here ...")
    df = df.apply(get_bbl, axis=1, result_type="expand")
    print("geocoding finished ...")
    return df


######### PTS #########################


def get_address(bbl):
    try:
        geo = g["BL"](bbl=bbl)
        addresses = geo.get("LIST OF GEOGRAPHIC IDENTIFIERS", "")
        filter_addresses = [
            d
            for d in addresses
            if d["Low House Number"] != "" and d["5-Digit Street Code"] != ""
        ]
        address = filter_addresses[0]
        b5sc = address.get("Borough Code", "0") + address.get(
            "5-Digit Street Code", "00000"
        )
        sname = get_sname(b5sc)
        numberOfExistingStructures = geo.get("Number of Existing Structures on Lot", "")
        hnum = address.get("Low House Number", "")
        return dict(
            sname=sname,
            hnum=hnum,
            numberOfExistingStructures=numberOfExistingStructures,
        )
    except:  # noqa: E722
        return dict(sname="", hnum="", numberOfExistingStructures="")


def get_sname(b5sc):
    try:
        geo = g["D"](B5SC=b5sc)
        return geo.get("First Street Name Normalized", "")
    except:  # noqa: E722
        return ""


def geocode(record):
    boro = record["boro"]
    block = record["block"]
    lot = record["lot"]
    ease = ""

    bbl = boro + block + lot
    if not isinstance(bbl, str) and len(bbl) == 10:
        raise ValueError(
            f"Malformatted bbl. Boro '{boro}', block '{block}', lot '{lot}'"
        )

    address = get_address(bbl)

    sname = address.get("sname", "")
    hnum = address.get("hnum", "")
    numberOfExistingStructures = address.get("numberOfExistingStructures")
    extra = dict(
        borough=boro,
        block=block,
        lot=lot,
        easement=ease,
        input_hnum=hnum,
        input_sname=sname,
        numberOfExistingStructures=numberOfExistingStructures,
    )
    try:
        geo_regular = g["1A"](
            street_name=sname, house_number=hnum, borough=boro, mode="regular"
        )
        geo_extended = g["1E"](
            street_name=sname, house_number=hnum, borough=boro, mode="extended"
        )
        geo = {**geo_extended, **geo_regular}
        geo = parse_output(geo)
        geo.update(extra)
        return geo
    except GeosupportError as e1:
        try:
            geo = g["BL"](bbl=bbl)
            geo = parse_output(geo)
            geo.update(extra)
            return geo
        except GeosupportError:
            geo = parse_output(e1.result)
            geo.update(extra)
            return geo


def parse_output(geo):
    return dict(
        billingbbl=geo.get("Condominium Billing BBL"),
        bbl=geo.get("BOROUGH BLOCK LOT (BBL)").get("BOROUGH BLOCK LOT (BBL)"),
        cd=geo.get("COMMUNITY DISTRICT", {}).get("COMMUNITY DISTRICT"),
        ct2010=geo.get("2010 Census Tract"),
        cb2010=geo.get("2010 Census Block"),
        ct2020=geo.get("2020 Census Tract"),
        cb2020=geo.get("2020 Census Block"),
        schooldist=geo.get("Community School District"),
        council=geo.get("City Council District"),
        zipcode=geo.get("ZIP Code"),
        firecomp=(
            geo.get("Fire Company Type", "")
            + geo.get("Fire Company Number", "")  # e.g E219
            or None
        ),
        policeprct=geo.get("Police Precinct"),
        healthCenterDistrict=geo.get("Health Center District"),
        healthArea=geo.get("Health Area"),
        sanitdistrict=geo.get("Sanitation District"),
        sanitsub=geo.get("Sanitation Collection Scheduling Section and Subsection"),
        boePreferredStreetName=geo.get("BOE Preferred Street Name"),
        taxmap=geo.get("Tax Map Number Section & Volume"),
        sanbornMapIdentifier=geo.get("SBVP (SANBORN MAP IDENTIFIER)", {}).get(
            "SBVP (SANBORN MAP IDENTIFIER)"
        ),
        latitude=geo.get("Latitude") or None,
        longitude=geo.get("Longitude") or None,
        grc=geo.get("Geosupport Return Code (GRC)"),
        grc2=geo.get("Geosupport Return Code 2 (GRC 2)"),
        msg=geo.get("Message"),
        msg2=geo.get("Message 2"),
    )


def geocode_df_bbl(df: pd.DataFrame) -> pd.DataFrame:
    print("geocoding begins here ...")
    df = df.apply(geocode, axis=1, result_type="expand")
    print("geocoding finished ...")
    return df
