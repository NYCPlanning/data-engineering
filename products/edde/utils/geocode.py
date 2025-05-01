from geosupport import Geosupport, GeosupportError


class _GW:
    """Geosupport wrapper"""

    def __init__(self) -> None:
        self.g = None

    @property
    def geosupport(self):
        if not self.g:
            self.g = Geosupport()
        return self.g


_gw = _GW()


def get_geosupport_puma(address: dict) -> str:
    """Requires docker"""
    try:
        geocoded = _gw.geosupport["1E"](
            street_name=address["street_name"],
            house_number=address["address_num"],
            borough=address["borough"],
            mode="extended",
        )
        return geocoded["PUMA Code"]
    except GeosupportError:
        return None
