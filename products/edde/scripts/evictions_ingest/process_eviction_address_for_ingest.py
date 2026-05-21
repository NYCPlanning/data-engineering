import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import usaddress
from geosupport import Geosupport, GeosupportError

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.geo_helpers import borough_name_mapper


# Geosupport wrapper - kept local to this script to avoid geosupport dependency in main pipeline
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


def geocode_address(address: dict) -> dict | None:
    """
    Geocode an address using Geosupport and return coordinates.

    Requires docker with geosupport.

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


def load_and_prepare_evictions(input_file: str) -> pd.DataFrame:
    """
    Load evictions data and prepare it for geocoding.

    Filters out current year records (incomplete data) and cleans borough names.
    """
    print(f"Loading evictions data from {input_file}")
    evictions = pd.read_parquet(input_file)

    # Extract year from executed_date
    evictions["year"] = evictions.executed_date.apply(lambda x: x[-4:])

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
    evictions["borough"] = evictions["borough_name"].map(borough_name_mapper)

    # Ensure numeric columns are properly typed
    num_cols = ["latitude", "longitude"]
    for c in num_cols:
        evictions[c] = pd.to_numeric(evictions[c], errors="coerce")

    return evictions


def geocode_from_address(record) -> dict | None:
    """
    Geocode an eviction record using the address when lat/lon are missing.

    Using usaddress parser: https://usaddress.readthedocs.io/en/latest/

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


def geocode_evictions(evictions: pd.DataFrame) -> pd.DataFrame:
    """
    Geocode evictions records where lat/lon are missing.

    For records with valid lat/lon, keep them as-is.
    For records without valid lat/lon, attempt to geocode from the address.
    """
    geocoded = evictions.copy()

    # Identify records that need geocoding (missing or invalid lat/lon)
    needs_geocoding = geocoded["latitude"].isna() | geocoded["longitude"].isna()

    print(f"Total records: {len(geocoded)}")
    print(f"Records needing geocoding: {needs_geocoding.sum()}")

    # Apply geocoding to records that need it
    if needs_geocoding.sum() > 0:
        geocoded_results = geocoded[needs_geocoding].apply(geocode_from_address, axis=1)

        # Extract geocoded coordinates and update the dataframe
        for idx, result in geocoded_results.items():
            if result is not None:
                # Convert to float to ensure proper numeric types
                lat = result.get("latitude")
                lon = result.get("longitude")
                geocoded.loc[idx, "latitude"] = float(lat) if lat is not None else None
                geocoded.loc[idx, "longitude"] = float(lon) if lon is not None else None
                geocoded.loc[idx, "puma"] = result.get("puma")

        successfully_geocoded = geocoded_results.notna().sum()
        print(
            f"Geocoding complete. Records successfully geocoded: {successfully_geocoded}"
        )

    # Ensure latitude and longitude are numeric types
    geocoded["latitude"] = pd.to_numeric(geocoded["latitude"], errors="coerce")
    geocoded["longitude"] = pd.to_numeric(geocoded["longitude"], errors="coerce")

    return geocoded


def process_evictions_for_ingest(input_file: str, output_file: str):
    """
    Main function to process evictions data for ingest.

    Args:
        input_file: Path to input parquet file
        output_file: Path to output parquet file
    """
    evictions = load_and_prepare_evictions(input_file)
    geocoded_evictions = geocode_evictions(evictions)

    print(f"\nSaving geocoded data to {output_file}")
    geocoded_evictions.to_parquet(output_file, index=False)
    print(f"✓ Saved {len(geocoded_evictions)} records")

    return geocoded_evictions
