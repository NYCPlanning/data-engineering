#!/usr/bin/env python3
"""
Streaming Geocoder for Melissa Data

Processes addresses one at a time, writing results immediately to output.
Maintains a simple position file for resuming from crashes.

⏺ Current Geocoding Logic

  The geocoder uses Geosupport Function 1B to convert addresses (house number + street name + borough/zip) into BBLs and geographic attributes. It first attempts Function 1B in regular mode,
  and if that fails or returns a non-success code, retries with tpad mode. Function 1B returns BBL, BIN, coordinates, census tract, census block, and other geographic identifiers in a single
  call.

  If a BBL is successfully retrieved but census tract or census block data is missing, the geocoder makes a supplementary call to Geosupport Function BL using the BBL as input. Function BL
  specializes in BBL lookups and fills in any missing census geography, ensuring that every valid BBL has complete census tract and block information.

"""

import argparse
import csv
import sys
from pathlib import Path

import usaddress
from geosupport import Geosupport, GeosupportError

# Initialize Geosupport
g = Geosupport()


# Load neighborhood to borough mappings
def load_borough_map():
    """Load neighborhood to borough mapping from CSV file."""
    from collections import defaultdict

    borough_map = {
        "MANHATTAN": ["Manhattan"],
        "BRONX": ["Bronx"],
        "BROOKLYN": ["Brooklyn"],
        "QUEENS": ["Queens"],
        "STATEN ISLAND": ["Staten Island"],
        "NEW YORK": ["Manhattan"],
    }

    neighborhoods = defaultdict(list)
    csv_path = Path(__file__).parent / "neighborhoods_to_boroughs.csv"
    if csv_path.exists():
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                neighborhood = row["neighborhood"].upper()
                borough = row["borough"]
                neighborhoods[neighborhood].append(borough)

    for neighborhood, boroughs in neighborhoods.items():
        borough_map[neighborhood] = boroughs

    return borough_map


BOROUGH_MAP = load_borough_map()

# Non-NYC cities to exclude
NON_NYC_CITIES = {
    "YONKERS",
    "MOUNT VERNON",
    "NEW ROCHELLE",
    "WHITE PLAINS",
    "PORT CHESTER",
    "RYE",
    "SCARSDALE",
    "MAMARONECK",
    "LARCHMONT",
    "PELHAM",
    "BRONXVILLE",
    "HEMPSTEAD",
    "GREAT NECK",
    "GLEN COVE",
    "LONG BEACH",
    "VALLEY STREAM",
    "JERSEY CITY",
    "NEWARK",
    "HOBOKEN",
    "BAYONNE",
}


def parse_address_components(address):
    """Parse address into house number and street name using usaddress library."""
    if not address:
        return None, None

    try:
        parsed = usaddress.parse(address)
        hnum_parts = [
            k for (k, v) in parsed if v in ("AddressNumber", "AddressNumberSuffix")
        ]
        house_number = "".join(hnum_parts) if hnum_parts else None

        sname_parts = [
            k
            for (k, v) in parsed
            if v
            in (
                "StreetName",
                "StreetNamePreDirectional",
                "StreetNamePostDirectional",
                "StreetNamePreType",
                "StreetNamePostType",
            )
        ]
        street_name = " ".join(sname_parts) if sname_parts else None
        return house_number, street_name
    except Exception:
        parts = address.split(None, 1) if address else []
        house_number = parts[0] if len(parts) > 0 else None
        street_name = parts[1] if len(parts) > 1 else None
        return house_number, street_name


def normalize_borough(city):
    """Normalize city name to borough name(s) for Geosupport."""
    if not city:
        return None
    city_upper = city.upper().strip()
    return BOROUGH_MAP.get(city_upper, None)


def geocode_address(record):
    """Geocode a single address record."""
    address = record.get("Address", "").strip()
    city = record.get("City", "").strip()
    state = record.get("State", "").strip()
    zip_code = record.get("Zip", "").strip()

    # Skip non-NYC addresses
    if state.upper() not in ("NY", "NEW YORK", ""):
        return {
            "bbl": "",
            "boro_code": "",
            "bin": "",
            "latitude": "",
            "longitude": "",
            "ct2020": "",
            "cb2020": "",
            "bctcb2020": "",
            "grc": "NON_NYC",
            "message": f"Non-NYC state: {state}",
        }

    if city.upper() in NON_NYC_CITIES:
        return {
            "bbl": "",
            "boro_code": "",
            "bin": "",
            "latitude": "",
            "longitude": "",
            "ct2020": "",
            "cb2020": "",
            "bctcb2020": "",
            "grc": "NON_NYC",
            "message": f"Non-NYC city: {city}",
        }

    house_number, street_name = parse_address_components(address)
    boroughs = normalize_borough(city)

    if not house_number or not street_name:
        return {
            "bbl": "",
            "boro_code": "",
            "bin": "",
            "latitude": "",
            "longitude": "",
            "ct2020": "",
            "cb2020": "",
            "bctcb2020": "",
            "grc": "PARSE_ERROR",
            "message": "Could not parse address",
        }

    geo_regular = None
    geo_tpad = None

    # Try geocoding with each possible borough
    if boroughs:
        for borough in boroughs:
            try:
                result = g["1B"](
                    street_name=street_name,
                    house_number=house_number,
                    borough=borough,
                    mode="regular",
                )
                if result.get("Geosupport Return Code (GRC)") == "00":
                    geo_regular = result
                    break
                elif not geo_regular:
                    geo_regular = result
            except GeosupportError as e:
                if e.result and not geo_regular:
                    geo_regular = e.result
            except Exception:
                pass

        if not geo_regular or geo_regular.get("Geosupport Return Code (GRC)") != "00":
            for borough in boroughs:
                try:
                    result = g["1B"](
                        street_name=street_name,
                        house_number=house_number,
                        borough=borough,
                        mode="tpad",
                    )
                    if result.get("Geosupport Return Code (GRC)") == "00":
                        geo_tpad = result
                        break
                    elif not geo_tpad:
                        geo_tpad = result
                except GeosupportError as e:
                    if e.result and not geo_tpad:
                        geo_tpad = e.result
                except Exception:
                    pass
    else:
        # Use zip code
        try:
            geo_regular = g["1B"](
                street_name=street_name,
                house_number=house_number,
                zip_code=zip_code,
                mode="regular",
            )
        except GeosupportError as e:
            geo_regular = e.result if e.result else None
        except Exception:
            pass

        if not geo_regular or geo_regular.get("Geosupport Return Code (GRC)") != "00":
            try:
                geo_tpad = g["1B"](
                    street_name=street_name,
                    house_number=house_number,
                    zip_code=zip_code,
                    mode="tpad",
                )
            except GeosupportError as e:
                geo_tpad = e.result if e.result else None
            except Exception:
                pass

    geo = (
        geo_regular
        if (geo_regular and geo_regular.get("Geosupport Return Code (GRC)") == "00")
        else geo_tpad
    )
    if not geo:
        geo = geo_regular if geo_regular else geo_tpad

    if not geo:
        return {
            "bbl": "",
            "boro_code": "",
            "bin": "",
            "latitude": "",
            "longitude": "",
            "ct2020": "",
            "cb2020": "",
            "bctcb2020": "",
            "grc": "",
            "message": "No geosupport response",
        }

    bbl = geo.get("BOROUGH BLOCK LOT (BBL)", {}).get("BOROUGH BLOCK LOT (BBL)")
    boro_code = geo.get("BOROUGH BLOCK LOT (BBL)", {}).get("Borough Code")
    ct2020 = geo.get("2020 Census Tract")
    cb2020 = geo.get("2020 Census Block")

    # Debug: log when we have BBL but missing census data
    if bbl and (not ct2020 or not cb2020):
        grc = geo.get("Geosupport Return Code (GRC)", "")
        print(
            f"DEBUG - BBL {bbl} missing census: ct2020={ct2020}, cb2020={cb2020}, GRC={grc}",
            file=sys.stderr,
        )

    # If we got a BBL but no census data, try Function BL to get census info
    if bbl and (not ct2020 or not cb2020):
        try:
            geo_bl = g["BL"](bbl=bbl)
            # Extract census data from BL result - BL should always succeed with valid BBL
            print(
                f"DEBUG - BL call succeeded for BBL {bbl}, GRC={geo_bl.get('Geosupport Return Code (GRC)')}",
                file=sys.stderr,
            )
            if not ct2020:
                ct2020 = geo_bl.get("2020 Census Tract")
            if not cb2020:
                cb2020 = geo_bl.get("2020 Census Block")
            print(
                f"DEBUG - After BL: ct2020={ct2020}, cb2020={cb2020}", file=sys.stderr
            )
            # Also get lat/lon from BL if we don't have them
            if not geo.get("Latitude"):
                geo_bl.get("Latitude")
                geo_bl.get("Longitude")
        except GeosupportError as e:
            print(f"DEBUG - BL GeosupportError for BBL {bbl}: {e}", file=sys.stderr)
            # Even on error, BL might return census data in the result
            if e.result:
                if not ct2020:
                    ct2020 = e.result.get("2020 Census Tract")
                if not cb2020:
                    cb2020 = e.result.get("2020 Census Block")
                print(
                    f"DEBUG - After BL error result: ct2020={ct2020}, cb2020={cb2020}",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"DEBUG - BL Exception for BBL {bbl}: {e}", file=sys.stderr)
            pass

    # Convert None to empty string for CSV output
    bbl = str(bbl) if bbl else ""
    boro_code = str(boro_code) if boro_code else ""
    ct2020 = str(ct2020) if ct2020 else ""
    cb2020 = str(cb2020) if cb2020 else ""

    bctcb2020 = ""
    if boro_code and ct2020 and cb2020:
        bctcb2020 = f"{boro_code}{ct2020}{cb2020}"

    bin_val = geo.get("Building Identification Number (BIN) of Input Address or NAP")
    lat = geo.get("Latitude")
    lon = geo.get("Longitude")

    return {
        "bbl": bbl,
        "boro_code": boro_code,
        "bin": str(bin_val) if bin_val else "",
        "latitude": str(lat) if lat else "",
        "longitude": str(lon) if lon else "",
        "ct2020": ct2020,
        "cb2020": cb2020,
        "bctcb2020": bctcb2020,
        "grc": str(geo.get("Geosupport Return Code (GRC)", "")),
        "message": str(geo.get("Message", "")),
    }


def main():
    parser = argparse.ArgumentParser(description="Stream geocode Melissa data")
    parser.add_argument("input_file", type=Path, help="Input pipe-delimited file")
    parser.add_argument("output_file", type=Path, help="Output pipe-delimited file")
    parser.add_argument(
        "--start-row", type=int, default=0, help="Start from this row (0-indexed)"
    )
    parser.add_argument(
        "--keep-non-nyc", action="store_true", help="Keep non-NYC addresses"
    )

    args = parser.parse_args()

    # Position file to track progress
    position_file = args.output_file.parent / f".{args.output_file.stem}_position"

    # Check if resuming
    start_row = args.start_row
    if start_row == 0 and position_file.exists():
        try:
            with open(position_file, "r") as f:
                start_row = int(f.read().strip())
            print(f"Resuming from row {start_row}")
        except Exception:
            pass

    # Determine if we're starting fresh or appending
    output_exists = args.output_file.exists() and start_row > 0
    write_mode = "a" if output_exists else "w"

    print(f"Input: {args.input_file}")
    print(f"Output: {args.output_file}")
    print(f"Starting from row: {start_row}")
    print(f"Mode: {'Appending' if output_exists else 'Writing new file'}")

    # Open input file
    with open(args.input_file, "r") as infile:
        reader = csv.DictReader(infile, delimiter="|")
        input_fieldnames = reader.fieldnames

        # Define output fieldnames
        geocode_fields = [
            "bbl",
            "boro_code",
            "bin",
            "latitude",
            "longitude",
            "ct2020",
            "cb2020",
            "bctcb2020",
            "grc",
            "message",
        ]
        output_fieldnames = list(input_fieldnames) + geocode_fields

        # Open output file
        args.output_file.parent.mkdir(parents=True, exist_ok=True)
        outfile = open(args.output_file, write_mode, newline="")
        writer = csv.DictWriter(outfile, fieldnames=output_fieldnames, delimiter="|")

        # Write header only if starting fresh
        if write_mode == "w":
            writer.writeheader()

        try:
            current_row = 0
            processed = 0
            success_count = 0
            census_count = 0

            for row in reader:
                # Skip rows before start_row
                if current_row < start_row:
                    current_row += 1
                    continue

                # Geocode
                geocoded = geocode_address(row)

                # Filter non-NYC if requested
                if not args.keep_non_nyc and geocoded.get("grc") == "NON_NYC":
                    current_row += 1
                    continue

                # Combine original + geocoded
                output_row = {**row, **geocoded}
                writer.writerow(output_row)
                outfile.flush()  # Ensure it's written immediately

                # Update stats
                processed += 1
                bbl = geocoded.get("bbl", "").strip()
                ct2020 = geocoded.get("ct2020", "").strip()

                if bbl:
                    success_count += 1
                if ct2020:
                    census_count += 1

                current_row += 1

                # Progress update every 1000 rows
                if processed % 1000 == 0:
                    print(
                        f"Row {current_row}: Processed {processed} | BBL: {success_count} ({success_count / processed * 100:.1f}%) | Census: {census_count} ({census_count / processed * 100:.1f}%)"
                    )

                    # Update position file
                    with open(position_file, "w") as f:
                        f.write(str(current_row))

        except KeyboardInterrupt:
            print(f"\nInterrupted at row {current_row}")
            print(
                f"To resume: python3 {__file__} {args.input_file} {args.output_file} --start-row {current_row}"
            )
        except Exception as e:
            print(f"\nError at row {current_row}: {e}", file=sys.stderr)
            raise
        finally:
            outfile.close()

            # Final position update
            with open(position_file, "w") as f:
                f.write(str(current_row))

            print("\nFinal stats:")
            print(f"  Last row processed: {current_row}")
            print(f"  Total processed: {processed}")
            print(
                f"  BBL success: {success_count} ({success_count / processed * 100:.1f}%)"
                if processed
                else ""
            )
            print(
                f"  Census success: {census_count} ({census_count / processed * 100:.1f}%)"
                if processed
                else ""
            )
            print(f"\nPosition saved to: {position_file}")
            print(f"To resume: python3 {__file__} {args.input_file} {args.output_file}")


if __name__ == "__main__":
    main()
