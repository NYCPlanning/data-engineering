#!/usr/bin/env python3
"""
Convert CSCL ETL Extractor Configuration XML to text formatting CSV.

Usage:
    python xml_to_text_formatting_csv.py <xml_file> <output_csv>
"""

import csv
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_justify_and_fill(field):
    """Parse justification and fillchar into justify_and_fill format."""
    justification = field.get("justification", "")
    fillchar = field.get("fillchar", " ")

    # Determine justification letter
    if justification == "LeftJustified":
        justify = "LJ"
    elif justification == "RightJustified":
        justify = "RJ"
    else:
        justify = "RJ"  # default

    # Determine fill character letter
    if fillchar == "0":
        fill = "ZF"
    elif fillchar == " ":
        fill = "SF"
    else:
        fill = "SF"  # default to space fill

    return f"{justify}{fill}"


def parse_blank_if_none(field):
    """Parse blankifnull attribute to TRUE/FALSE."""
    blankifnull = field.get("blankifnull", "false").lower()
    return "TRUE" if blankifnull == "true" else "FALSE"


def convert_field_name(name):
    """Convert XML field name to snake_case."""
    # Simple conversion - just lowercase and replace spaces with underscores
    return name.lower().replace(" ", "_").replace("-", "_")


def xml_to_csv(xml_path: Path, output_csv: Path) -> None:
    """Convert XML extractor configuration to text formatting CSV."""

    # Parse XML
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Handle namespace
    ns = {"ns": "http://bownegroup.com/ExtractorConfiguration.xsd"}
    file_fields = root.find(".//ns:FileFields", ns)

    if file_fields is None:
        # Try without namespace
        file_fields = root.find(".//FileFields")

    if file_fields is None:
        print("Error: Could not find FileFields in XML")
        sys.exit(1)

    # Extract fields
    rows = []
    current_position = 1

    for field in file_fields.findall(".//ns:FileField", ns) or file_fields.findall(
        ".//FileField"
    ):
        field_number = field.get("name", "")
        field_label = field.get("description", "")
        field_name = convert_field_name(field_label)
        field_length = int(field.get("length", "0"))
        justify_and_fill = parse_justify_and_fill(field)
        blank_if_none = parse_blank_if_none(field)

        start_index = current_position
        end_index = current_position + field_length - 1

        rows.append(
            {
                "field_number": field_number,
                "field_name": field_name,
                "field_label": field_label,
                "field_length": field_length,
                "start_index": start_index,
                "end_index": end_index,
                "justify_and_fill": justify_and_fill,
                "blank_if_none": blank_if_none,
            }
        )

        current_position += field_length

    # Write CSV
    fieldnames = [
        "field_number",
        "field_name",
        "field_label",
        "field_length",
        "start_index",
        "end_index",
        "justify_and_fill",
        "blank_if_none",
    ]

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Converted {len(rows)} fields from XML to CSV")
    print(f"  Output: {output_csv}")
    print(f"  Total length: {current_position - 1}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python xml_to_text_formatting_csv.py <xml_file> <output_csv>")
        sys.exit(1)

    xml_file = Path(sys.argv[1])
    output_csv = Path(sys.argv[2])

    if not xml_file.exists():
        print(f"Error: XML file not found: {xml_file}")
        sys.exit(1)

    xml_to_csv(xml_file, output_csv)
