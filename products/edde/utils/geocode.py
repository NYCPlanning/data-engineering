from geosupport import Geosupport, GeosupportError
import pandas as pd
import usaddress


class GW:
    """Geosupport wrapper"""

    def __init__(self) -> None:
        self.g = None

    @property
    def geosupport(self):
        if not self.g:
            self.g = Geosupport()
        return self.g


gw = GW()


def from_eviction_address(record) -> str:
    """Return latitude, longitude in degrees"""
    address = eviction_record_to_address(record)
    return geocode_address(address)


def eviction_record_to_address(record) -> dict:
    """Using these docs as guide https://usaddress.readthedocs.io/en/latest/"""
    parsed = usaddress.parse(record.eviction_address)
    parsed = {k: v for v, k in parsed}
    rv = {}
    rv["address_num"] = parsed.get("AddressNumber", "")
    street_name_components = [
        parsed.get("StreetNamePreModifier"),
        parsed.get("StreetNamePreDirectional"),
        parsed.get("StreetNamePreType"),
        parsed.get("StreetName"),
        parsed.get("StreetNamePostModifier"),
        parsed.get("StreetNamePostDirectional"),
        parsed.get("StreetNamePostType"),
    ]
    rv["street_name"] = " ".join([s for s in street_name_components if s])
    rv["borough"] = record.borough
    rv["zip"] = record.eviction_postcode
    return rv


def geocode_address(address: dict) -> str:
    """Requires docker"""
    try:
        geocoded = gw.geosupport["1E"](
            street_name=address["street_name"],
            house_number=address["address_num"],
            borough=address["borough"],
            mode="extended",
        )
        return geocoded["PUMA Code"]
    except GeosupportError as e:
        geo = e.result
        return None
