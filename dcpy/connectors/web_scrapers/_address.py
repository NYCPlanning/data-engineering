from dataclasses import dataclass
import usaddress


@dataclass
class AddressComponents:
    place_name: str | None = None
    address_line_1: str | None = None
    address_line_2: str | None = None
    building_name: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None


def parse(address_string: str):
    parsed_address, address_type = usaddress.tag(address_string)

    place_name = parsed_address.get("Recipient")

    address_line_1_keys = [
        "AddressNumber",
        "AddressNumberPrefix",
        "AddressNumberSuffix",
        "StreetNamePreDirectional",
        "StreetNamePreModifier",
        "StreetNamePreType",
        "StreetName",
        "StreetNamePostType",
        "StreetNamePostDirectional",
        "StreetNamePostModifier",
    ]

    address_line_1_parts = [
        parsed_address.get(key)
        for key in address_line_1_keys
        if parsed_address.get(key)
    ]
    street_address = " ".join(address_line_1_parts) if address_line_1_parts else None

    building_name = parsed_address.get("BuildingName")

    address_line_2_parts = []
    if parsed_address.get("OccupancyType") and parsed_address.get(
        "OccupancyIdentifier"
    ):
        address_line_2_parts.append(
            f"{parsed_address.get('OccupancyType')} {parsed_address.get('OccupancyIdentifier')}"
        )
    address_line_2 = " ".join(address_line_2_parts) if address_line_2_parts else None

    city = parsed_address.get("PlaceName")
    state = parsed_address.get("StateName")
    zip_code = parsed_address.get("ZipCode")

    return AddressComponents(
        place_name=place_name,
        address_line_1=street_address,
        address_line_2=address_line_2,
        building_name=building_name,
        city=city,
        state=state,
        zip_code=zip_code,
    )
