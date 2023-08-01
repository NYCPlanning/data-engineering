from multiprocessing import Pool, cpu_count
from geosupport import Geosupport, GeosupportError
from datetime import date
import pandas as pd
import json
import os

g = Geosupport()


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
    except:
        return dict(sname="", hnum="", numberOfExistingStructures="")


def get_sname(b5sc):
    try:
        geo = g["D"](B5SC=b5sc)
        return geo.get("First Street Name Normalized", "")
    except:
        return ""


def geocode(inputs):
    boro = inputs.pop("boro")
    block = inputs.pop("block")
    lot = inputs.pop("lot")
    ease = ""

    bbl = boro + block + lot
    address = get_address(bbl)

    sname = address.get("sname", "")
    hnum = address.get("hnum", "")
    numberOfExistingStructures = address.get("numberOfExistingStructures", "")
    borough = boro
    extra = dict(
        borough=borough,
        block=block,
        lot=lot,
        easement=ease,
        input_hnum=hnum,
        input_sname=sname,
        numberOfExistingStructures=numberOfExistingStructures,
    )
    try:
        geo_regular = g["1A"](
            street_name=sname, house_number=hnum, borough=borough, mode="regular"
        )
        geo_extended = g["1E"](
            street_name=sname, house_number=hnum, borough=borough, mode="extended"
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
        except GeosupportError as e2:
            geo = parse_output(e1.result)
            geo.update(extra)
            return geo


def parse_output(geo):
    return dict(
        billingbbl=geo.get("Condominium Billing BBL", ""),
        bbl=geo.get("BOROUGH BLOCK LOT (BBL)", "").get("BOROUGH BLOCK LOT (BBL)", ""),
        cd=geo.get("COMMUNITY DISTRICT", {}).get("COMMUNITY DISTRICT", ""),
        ct2010=geo.get("2010 Census Tract", ""),
        cb2010=geo.get("2010 Census Block", ""),
        ct2020=geo.get("2020 Census Tract", ""),
        cb2020=geo.get("2020 Census Block", ""),
        schooldist=geo.get("Community School District", ""),
        council=geo.get("City Council District", ""),
        zipcode=geo.get("ZIP Code", ""),
        firecomp=geo.get("Fire Company Type", "")
        + geo.get("Fire Company Number", ""),  # e.g E219
        policeprct=geo.get("Police Precinct", ""),
        healthCenterDistrict=geo.get("Health Center District", ""),
        healthArea=geo.get("Health Area", ""),
        sanitdistrict=geo.get("Sanitation District", ""),
        sanitsub=geo.get("Sanitation Collection Scheduling Section and Subsection", ""),
        boePreferredStreetName=geo.get("BOE Preferred Street Name", ""),
        taxmap=geo.get("Tax Map Number Section & Volume", ""),
        sanbornMapIdentifier=geo.get("SBVP (SANBORN MAP IDENTIFIER)", {}).get(
            "SBVP (SANBORN MAP IDENTIFIER)", ""
        ),
        latitude=geo.get("Latitude", ""),
        longitude=geo.get("Longitude", ""),
        xcoord="",
        ycoord="",
        grc=geo.get("Geosupport Return Code (GRC)", ""),
        grc2=geo.get("Geosupport Return Code 2 (GRC 2)", ""),
        msg=geo.get("Message", ""),
        msg2=geo.get("Message 2", ""),
    )


if __name__ == "__main__":
    df = pd.read_csv("geocode_input_pluto_pts.csv", dtype=str, index_col=False)
    # get the row number
    records = df.to_dict("records")

    print("geocoding begins here ...")

    # Multiprocess
    with Pool(processes=cpu_count()) as pool:
        it = pool.map(geocode, records, chunksize=100000)

    print("geocoding finished ...")
    result = pd.DataFrame(it)
    del it
    print(result.head())

    result.to_csv("pluto_input_geocodes.csv", index=False)
