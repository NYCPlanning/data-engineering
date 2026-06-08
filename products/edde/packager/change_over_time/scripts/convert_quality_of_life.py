#!/usr/bin/env python3
"""
Convert quality_of_life data to change-over-time format.
Python port of convert-qol.js
"""

import sys
from pathlib import Path

# Add parent directory to path to import calculator
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from change_over_time.calculator import ChangeCalculator
from change_over_time.config import quality_of_life


def convert_quality_of_life(
    input_csv: Path,
    output_csv: Path,
    geography_col: str = "borough",
):
    """
    Convert quality_of_life input CSV to change-over-time output.

    Args:
        input_csv: Path to input CSV (e.g., quality_of_life_borough.csv)
        output_csv: Path to output CSV (e.g., qol_change_borough.csv)
        geography_col: Geography column name ('puma', 'borough', or 'citywide')
    """
    calculator = ChangeCalculator(
        input_csv=input_csv,
        labels=quality_of_life.LABELS,
        geography_col=geography_col,
        old_yearband="0812",
        new_yearband="1923",
        special_yearbands=quality_of_life.SPECIAL_YEARBANDS,
    )

    result_df = calculator.run()

    # Write output
    result_df.to_csv(output_csv, index=False)
    print(f"✓ Converted {input_csv.name}")
    print(f"  → {output_csv}")
    print(f"  Rows: {len(result_df)}")


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python convert_quality_of_life.py <input.csv> <output.csv> [geography_col]"
        )
        print()
        print("Examples:")
        print(
            "  python convert_quality_of_life.py quality_of_life_borough.csv output.csv borough"
        )
        print(
            "  python convert_quality_of_life.py quality_of_life_puma.csv output.csv puma"
        )
        sys.exit(1)

    input_csv = Path(sys.argv[1])
    output_csv = Path(sys.argv[2])
    geography_col = sys.argv[3] if len(sys.argv) > 3 else "borough"

    if not input_csv.exists():
        print(f"Error: Input file not found: {input_csv}")
        sys.exit(1)

    convert_quality_of_life(input_csv, output_csv, geography_col)


if __name__ == "__main__":
    main()
