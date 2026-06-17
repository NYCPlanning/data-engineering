"""Geocode DOI evictions data using Geosupport.

This script processes evictions data to geocode addresses where lat/lon are missing,
using Geosupport for address matching. It also filters out current year records
(incomplete data) and adds borough and year information.
"""

from datetime import datetime

import pandas as pd
import usaddress
from geosupport import Geosupport, GeosupportError

# Borough name to code mapping
BOROUGH_NAME_MAPPER = {
    "Bronx": "BX",
    "Brooklyn": "BK",
    "Manhattan": "MN",
    "Queens": "QN",
    "Staten Island": "SI",
}


class _GeosupportWrapper:
    """Geosupport wrapper - lazy initialization to avoid loading until needed."""

    def __init__(self) -> None:
        self.g = None

    @property
    def geosupport(self):
        if not self.g:
            self.g = Geosupport()
        return self.g


_gw = _GeosupportWrapper()


def geocode_address(address: dict) -> dict | None:
    """Geocode an address using Geosupport and return coordinates.

    Args:
        address: Dictionary with 'street_name', 'address_num', 'borough', 'zip'

    Returns:
        dict: Dictionary with 'latitude', 'longitude', 'puma' if successful
        None: If geocoding fails
    """
    try:
        geocoded = _gw.geosupport["1E"](
            street_name=address["street_name"],
            house_number=address["address_num"],
            borough=address["borough"],
            mode="extended",
        )
        return {
            "latitude": geocoded.get("Latitude"),
            "longitude": geocoded.get("Longitude"),
            "puma": geocoded.get("PUMA Code"),
        }
    except GeosupportError:
        return None


def geocode_from_address(record) -> dict | None:
    """Geocode an eviction record using the address when lat/lon are missing.

    Uses usaddress parser: https://usaddress.readthedocs.io/en/latest/

    Args:
        record: Pandas Series with eviction_address, borough, eviction_postcode

    Returns:
        dict: Dictionary containing geocoded information (latitude, longitude, puma)
        None: If geocoding fails
    """
    try:
        parsed = usaddress.parse(record.eviction_address)
        parsed = {k: v for v, k in parsed}

        address_parts = {
            "address_num": parsed.get("AddressNumber", ""),
            "street_name": " ".join(
                filter(
                    None,
                    [
                        parsed.get("StreetNamePreModifier"),
                        parsed.get("StreetNamePreDirectional"),
                        parsed.get("StreetNamePreType"),
                        parsed.get("StreetName"),
                        parsed.get("StreetNamePostModifier"),
                        parsed.get("StreetNamePostDirectional"),
                        parsed.get("StreetNamePostType"),
                    ],
                )
            ),
            "borough": record.borough,
            "zip": record.eviction_postcode,
        }

        return geocode_address(address_parts)
    except Exception:
        # If geocoding fails, return None
        return None


def process_evictions(df: pd.DataFrame) -> pd.DataFrame:
    """Process and geocode evictions data.

    This function:
    1. Extracts year from executed_date
    2. Filters out current year records (incomplete data)
    3. Cleans and standardizes borough names
    4. Geocodes records missing lat/lon using Geosupport
    5. Adds puma, borough_name, and year columns

    Args:
        df: Input DataFrame from doi_evictions dataset

    Returns:
        DataFrame with geocoded data and additional columns
    """
    evictions = df.copy()

    # Extract year from executed_date (last 4 characters)
    evictions["year"] = evictions.executed_date.apply(lambda x: x[-4:] if x else None)

    # Get current year and exclude those records (incomplete data)
    current_year = str(datetime.now().year)
    evictions = evictions[evictions["year"] != current_year].copy()

    # Clean borough names
    evictions["borough_name"] = (
        evictions["borough"].str[0] + evictions["borough"].str[1:].str.lower()
    )
    evictions["borough_name"] = evictions["borough_name"].replace(
        {"Staten island": "Staten Island"}
    )
    evictions["borough"] = evictions["borough_name"].map(BOROUGH_NAME_MAPPER)

    # Ensure numeric columns are properly typed
    num_cols = ["latitude", "longitude"]
    for c in num_cols:
        evictions[c] = pd.to_numeric(evictions[c], errors="coerce")

    # Identify records that need geocoding (missing or invalid lat/lon)
    needs_geocoding = evictions["latitude"].isna() | evictions["longitude"].isna()

    print(f"Total records after filtering current year: {len(evictions)}")
    print(f"Records needing geocoding: {needs_geocoding.sum()}")

    # Apply geocoding to records that need it
    if needs_geocoding.sum() > 0:
        geocoded_results = evictions[needs_geocoding].apply(  # type: ignore[call-overload]
            geocode_from_address, axis=1
        )

        # Extract geocoded coordinates and update the dataframe
        for idx, result in geocoded_results.items():
            if result is not None:
                # Convert to float to ensure proper numeric types
                lat = result.get("latitude")
                lon = result.get("longitude")
                evictions.loc[idx, "latitude"] = float(lat) if lat is not None else None
                evictions.loc[idx, "longitude"] = (
                    float(lon) if lon is not None else None
                )
                evictions.loc[idx, "puma"] = result.get("puma")

        successfully_geocoded = geocoded_results.notna().sum()
        print(f"Geocoding complete. Successfully geocoded: {successfully_geocoded}")
    else:
        # Initialize puma column if no geocoding needed
        evictions["puma"] = None

    # Ensure latitude and longitude are numeric types
    evictions["latitude"] = pd.to_numeric(evictions["latitude"], errors="coerce")
    evictions["longitude"] = pd.to_numeric(evictions["longitude"], errors="coerce")

    return evictions
