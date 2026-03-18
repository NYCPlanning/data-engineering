#!/usr/bin/env python3
"""
Generate a visual header for fixed-width formatted data files.

This helps debug and compare expected vs actual data by showing field boundaries
and field numbers in a format that aligns with the actual data.

Usage:
    python generate_field_header.py <text_formatting_csv>
"""

import csv
import sys
from pathlib import Path


def generate_header(csv_path: Path) -> None:
    """Generate a visual header showing field positions and numbers."""

    # Read the CSV
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Build the header lines
    separator_parts = []
    number_parts = []
    name_parts = []
    pipe_parts = []

    for row in rows:
        field_num = row["field_number"]
        field_name = row["field_name"]
        field_length = int(row["field_length"])

        # For single character fields, use a compact representation
        if field_length == 1:
            # Use just the last digit of field number for single chars
            num_display = field_num.replace("TL", "")[-1]
            separator_parts.append(num_display)
            number_parts.append(num_display)
            name_parts.append("·")  # Use middot for single char fields
            pipe_parts.append("+")  # Plus for single char fields
        else:
            # Multi-character fields
            available = field_length
            # Remove TL prefix from field number
            num_display = field_num.replace("TL", "")

            # Center the field number in the available space
            if len(num_display) <= available:
                padding = available - len(num_display)
                left_pad = padding // 2
                right_pad = padding - left_pad
                centered_num = " " * left_pad + num_display + " " * right_pad
            else:
                # If number doesn't fit, truncate
                centered_num = num_display[:available]

            # Create separator with dashes and field number at center
            sep_line = "-" * field_length

            # Create pipe line with | at start, spaces in middle
            pipe_line = "|" + " " * (field_length - 1)

            separator_parts.append(sep_line)
            number_parts.append(centered_num)
            pipe_parts.append(pipe_line)

            # Center the field name (or truncate if too long)
            if len(field_name) <= available:
                padding = available - len(field_name)
                left_pad = padding // 2
                right_pad = padding - left_pad
                name_parts.append(" " * left_pad + field_name + " " * right_pad)
            else:
                # Truncate with ellipsis
                name_parts.append(field_name[: available - 1] + "…")

    # Print the header
    print("".join(pipe_parts) + "|")  # Add final pipe at end
    print("".join(separator_parts))
    print("".join(number_parts))
    print("".join(name_parts))
    print("".join(separator_parts))
    print("".join(pipe_parts) + "|")  # Add final pipe at end

    # Also print total length info
    total_length = int(rows[-1]["end_index"])
    print(f"\nTotal length: {total_length} characters")
    print(f"Total fields: {len(rows)}")

    # Print a ruler for character positions (every 10 chars)
    print("\nCharacter position ruler:")
    ruler_top = []
    ruler_bot = []
    for i in range(1, total_length + 1):
        if i % 10 == 0:
            ruler_top.append(str(i // 10))
            ruler_bot.append("0")
        elif i % 10 == 1 and i > 1:
            ruler_top.append(".")
            ruler_bot.append(str(i % 10))
        else:
            ruler_top.append(" ")
            ruler_bot.append(str(i % 10))

    print("".join(ruler_top))
    print("".join(ruler_bot))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_field_header.py <text_formatting_csv>")
        sys.exit(1)

    csv_file = Path(sys.argv[1])

    if not csv_file.exists():
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)

    generate_header(csv_file)
