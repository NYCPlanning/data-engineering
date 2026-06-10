#!/usr/bin/env python3
"""
Convert housing_security data to change-over-time format.
Python port of 2025-convert-housing_security.js
"""

from pathlib import Path

from packager.change_over_time.calculator import ChangeCalculator
from packager.change_over_time.config import housing_security


def convert_housing_security(
    input_csv: Path,
    output_csv: Path,
    geography_col: str = "borough",
    old_yearband: str = "0812",
    new_yearband: str = "2024",
):
    """
    Convert housing_security input CSV to change-over-time output.

    Args:
        input_csv: Path to input CSV (e.g., housing_security_borough.csv)
        output_csv: Path to output CSV (e.g., housing_security_change_borough.csv)
        geography_col: Geography column name ('puma', 'borough', or 'citywide')
        old_yearband: Old year band (default: "0812")
        new_yearband: New year band (default: "2024")
    """
    calculator = ChangeCalculator(
        input_csv=input_csv,
        labels=housing_security.LABELS,
        geography_col=geography_col,
        old_yearband=old_yearband,
        new_yearband=new_yearband,
        special_yearbands=housing_security.SPECIAL_YEARBANDS,
    )

    result_df = calculator.run()

    # Write output
    result_df.to_csv(output_csv, index=False)
    print(f"✓ Converted {input_csv.name}")
    print(f"  → {output_csv}")
    print(f"  Rows: {len(result_df)}")
