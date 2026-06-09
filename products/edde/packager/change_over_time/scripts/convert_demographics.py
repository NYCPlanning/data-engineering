#!/usr/bin/env python3
"""
Convert demographics data to change-over-time format.
Python port of convert-demo.js

NOTE: Demographics uses separate OLD and NEW input files, unlike other categories.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to path to import calculator
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from change_over_time.calculator import ChangeCalculator
from change_over_time.config import demographics


def convert_demographics(
    old_csv: Path,
    new_csv: Path,
    output_csv: Path,
    geography_col: str = "borough",
):
    """
    Convert demographics input CSVs to change-over-time output.

    Demographics data comes as separate OLD and NEW files that need to be combined.

    Args:
        old_csv: Path to OLD data CSV (e.g., demographics_0812_borough.csv)
        new_csv: Path to NEW data CSV (e.g., demographics_1923_borough.csv)
        output_csv: Path to output CSV (e.g., demo_change_borough.csv)
        geography_col: Geography column name ('puma', 'borough', or 'citywide')
    """
    # Read old and new dataframes
    old_df = pd.read_csv(old_csv, dtype={geography_col: str})
    new_df = pd.read_csv(new_csv, dtype={geography_col: str})

    # Rename columns to add _change_ suffix for calculator compatibility
    # The calculator expects columns like "pop_change_count" but the input has "pop_count"
    old_renamed = {geography_col: old_df[geography_col]}
    new_renamed = {geography_col: new_df[geography_col]}

    for label in demographics.LABELS:
        # Extract the base column name by removing "_change_"
        # e.g., "pop_change_count" -> "pop_count"
        parts = label.split("_change_")
        if len(parts) == 2:
            base_col = f"{parts[0]}_{parts[1]}"
        else:
            base_col = label

        # Copy main column and companions
        if base_col in old_df.columns:
            old_renamed[label] = old_df[base_col]
        else:
            old_renamed[label] = np.nan

        if base_col in new_df.columns:
            new_renamed[label] = new_df[base_col]
        else:
            new_renamed[label] = np.nan

        # Copy MOE and CV columns (but NOT _pct/_pct_moe as those will be calculated)
        for suffix in ["_moe", "_cv"]:
            old_col = f"{base_col}{suffix}"
            new_col = f"{base_col}{suffix}"
            label_col = f"{label}{suffix}"

            if old_col in old_df.columns:
                old_renamed[label_col] = old_df[old_col]
            else:
                old_renamed[label_col] = np.nan

            if new_col in new_df.columns:
                new_renamed[label_col] = new_df[new_col]
            else:
                new_renamed[label_col] = np.nan

        # For count variables, also copy the base _pct and _pct_moe for percentage point calculations
        # e.g., for "pop_change_count", copy "pop_pct" -> "pop_change_pct"
        # But NOT for median variables (they don't have corresponding _pct in input)
        if label.endswith("_count"):
            # Extract the base variable name without subgroup and without _count
            # e.g., "pop_change_anh_count" -> "pop_anh"
            base_for_pct = (
                base_col[:-6] if len(base_col) > 6 else base_col
            )  # Remove "_count"

            for pct_suffix in ["_pct", "_pct_moe"]:
                old_pct_col = f"{base_for_pct}{pct_suffix}"
                new_pct_col = f"{base_for_pct}{pct_suffix}"
                # Output column should be like "pop_change_pct" not "pop_change_count_pct"
                label_pct = (
                    label[:-6] + pct_suffix
                    if label.endswith("_count")
                    else f"{label}{pct_suffix}"
                )

                if old_pct_col in old_df.columns:
                    old_renamed[label_pct] = old_df[old_pct_col]
                else:
                    old_renamed[label_pct] = np.nan

                if new_pct_col in new_df.columns:
                    new_renamed[label_pct] = new_df[new_pct_col]
                else:
                    new_renamed[label_pct] = np.nan

    old_df_renamed = pd.DataFrame(old_renamed)
    new_df_renamed = pd.DataFrame(new_renamed)

    # Now use the calculator's calculate_change method directly
    calculator = ChangeCalculator(
        input_csv=old_csv,  # Dummy, won't be used
        labels=demographics.LABELS,
        geography_col=geography_col,
        decimal_change_labels=demographics.AGE_MEDIAN_LABELS,
        strip_suffix_for_derived=True,  # Demographics strips _median/_count from derived columns
    )

    result_df = calculator.calculate_change(old_df_renamed, new_df_renamed)

    # Write output
    result_df.to_csv(output_csv, index=False)
    print(f"✓ Converted {old_csv.name} + {new_csv.name}")
    print(f"  → {output_csv}")
    print(f"  Rows: {len(result_df)}")


def main():
    """Main entry point. Auto-discovers paths from build_metadata and recipe."""
    from change_over_time.paths import get_old_csv, get_new_csv, get_edde_paths

    if len(sys.argv) != 2:
        print("Usage: python convert_demographics.py <geography_col>")
        print()
        print("Examples:")
        print("  python convert_demographics.py borough")
        print("  python convert_demographics.py puma")
        print("  python convert_demographics.py citywide")
        sys.exit(1)

    geography_col = sys.argv[1]

    print(f"Auto-discovering paths for geography: {geography_col}")
    old_edde_path, new_build_path = get_edde_paths()
    print(f"  Old EDDE data: {old_edde_path}")
    print(f"  New build data: {new_build_path}")

    old_csv = get_old_csv("demographics", geography_col)
    new_csv = get_new_csv("demographics", geography_col)

    # Output to current build data directory
    output_csv = new_build_path / "demographics" / f"demographics_change_{geography_col}.csv"
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    print(f"  Old CSV: {old_csv}")
    print(f"  New CSV: {new_csv}")
    print(f"  Output: {output_csv}")

    convert_demographics(old_csv, new_csv, output_csv, geography_col)


if __name__ == "__main__":
    main()
