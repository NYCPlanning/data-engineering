#!/usr/bin/env python3
"""
Convert economics data to change-over-time format.
Python port of convert-econ.js

NOTE: Economics uses separate OLD and NEW input files, like demographics.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to path to import calculator
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from change_over_time.calculator import ChangeCalculator
from change_over_time.config import economics


def convert_economics(
    old_csv: Path,
    new_csv: Path,
    output_csv: Path,
    geography_col: str = "borough",
):
    """
    Convert economics input CSVs to change-over-time output.

    Economics data comes as separate OLD and NEW files that need to be combined.

    Args:
        old_csv: Path to OLD data CSV (e.g., economics_0812_borough.csv)
        new_csv: Path to NEW data CSV (e.g., economics_1923_borough.csv)
        output_csv: Path to output CSV (e.g., economics_change_borough.csv)
        geography_col: Geography column name ('puma', 'borough', or 'citywide')
    """
    # Read old and new dataframes
    old_df = pd.read_csv(old_csv, dtype={geography_col: str})
    new_df = pd.read_csv(new_csv, dtype={geography_col: str})

    # Rename columns to add _change_ suffix for calculator compatibility
    # The calculator expects columns like "age_p25pl_change_count" but input has "age_p25pl_count"
    old_renamed = {geography_col: old_df[geography_col]}
    new_renamed = {geography_col: new_df[geography_col]}

    # Economics uses simple label format but calculator expects _change_ format
    # Convert labels to _change_ format for internal processing
    for label in economics.LABELS:
        # Convert label to _change_ format: "age_p25pl_count" -> "age_p25pl_change_count"
        # Split on last underscore to get variable and type
        parts = label.rsplit("_", 1)
        if len(parts) == 2:
            variable, type_suffix = parts
            change_label = f"{variable}_change_{type_suffix}"
        else:
            change_label = f"{label}_change"

        # Copy main column
        if label in old_df.columns:
            old_renamed[change_label] = old_df[label]
        else:
            old_renamed[change_label] = np.nan

        if label in new_df.columns:
            new_renamed[change_label] = new_df[label]
        else:
            new_renamed[change_label] = np.nan

        # Copy MOE and CV columns
        for suffix in ["_moe", "_cv"]:
            old_col = f"{label}{suffix}"
            new_col = f"{label}{suffix}"
            change_col = f"{change_label}{suffix}"

            if old_col in old_df.columns:
                old_renamed[change_col] = old_df[old_col]
            else:
                old_renamed[change_col] = np.nan

            if new_col in new_df.columns:
                new_renamed[change_col] = new_df[new_col]
            else:
                new_renamed[change_col] = np.nan

        # For count variables, copy the base _pct and _pct_moe for percentage point calculations
        if label.endswith("_count"):
            # Extract the base variable name without _count
            # e.g., "age_p25pl_count" -> "age_p25pl"
            base_for_pct = label[:-6] if len(label) > 6 else label  # Remove "_count"
            # Convert to _change_ format
            change_base = change_label[:-6]  # Remove "_count" from change_label

            for pct_suffix in ["_pct", "_pct_moe"]:
                old_pct_col = f"{base_for_pct}{pct_suffix}"
                new_pct_col = f"{base_for_pct}{pct_suffix}"
                change_pct_col = f"{change_base}{pct_suffix}"

                if old_pct_col in old_df.columns:
                    old_renamed[change_pct_col] = old_df[old_pct_col]
                else:
                    old_renamed[change_pct_col] = np.nan

                if new_pct_col in new_df.columns:
                    new_renamed[change_pct_col] = new_df[new_pct_col]
                else:
                    new_renamed[change_pct_col] = np.nan

    old_df_renamed = pd.DataFrame(old_renamed)
    new_df_renamed = pd.DataFrame(new_renamed)

    # Convert labels to _change_ format for calculator
    change_labels = []
    for label in economics.LABELS:
        parts = label.rsplit("_", 1)
        if len(parts) == 2:
            variable, type_suffix = parts
            change_labels.append(f"{variable}_change_{type_suffix}")
        else:
            change_labels.append(f"{label}_change")

    # Use the calculator's calculate_change method
    calculator = ChangeCalculator(
        input_csv=old_csv,  # Dummy, won't be used
        labels=change_labels,
        geography_col=geography_col,
        top_bottom_codes=economics.TOP_BOTTOM_CODES,  # Pass top/bottom code configuration
        strip_suffix_for_derived=True,  # Economics strips suffix like demographics
    )

    result_df = calculator.calculate_change(old_df_renamed, new_df_renamed)

    # Rename columns to remove _change_ suffix (economics output format)
    column_mapping = {geography_col: geography_col}
    for col in result_df.columns:
        if col != geography_col:
            # Remove _change_ from column name
            new_col = col.replace("_change_", "_")
            column_mapping[col] = new_col

    result_df = result_df.rename(columns=column_mapping)

    # Write output
    result_df.to_csv(output_csv, index=False)
    print(f"✓ Converted {old_csv.name} + {new_csv.name}")
    print(f"  → {output_csv}")
    print(f"  Rows: {len(result_df)}")


def main():
    if len(sys.argv) < 4:
        print(
            "Usage: python convert_economics.py <old.csv> <new.csv> <output.csv> [geography_col]"
        )
        print()
        print("Examples:")
        print(
            "  python convert_economics.py economics_0812_borough.csv economics_1923_borough.csv output.csv borough"
        )
        print(
            "  python convert_economics.py economics_0812_puma.csv economics_1923_puma.csv output.csv puma"
        )
        sys.exit(1)

    old_csv = Path(sys.argv[1])
    new_csv = Path(sys.argv[2])
    output_csv = Path(sys.argv[3])
    geography_col = sys.argv[4] if len(sys.argv) > 4 else "borough"

    if not old_csv.exists():
        print(f"Error: Input file not found: {old_csv}")
        sys.exit(1)

    if not new_csv.exists():
        print(f"Error: Input file not found: {new_csv}")
        sys.exit(1)

    convert_economics(old_csv, new_csv, output_csv, geography_col)


if __name__ == "__main__":
    main()
