#!/usr/bin/env python3
"""
Run all change-over-time conversions for EDDE.

This script runs all category conversions (demographics, economics, housing_security, quality_of_life)
for all geographies (borough, puma, citywide).
"""

from packager.change_over_time.paths import (
    get_edde_paths,
    get_new_csv,
    get_old_csv,
    get_yearbands,
)
from packager.change_over_time.scripts.convert_demographics import convert_demographics
from packager.change_over_time.scripts.convert_economics import convert_economics
from packager.change_over_time.scripts.convert_housing_security import (
    convert_housing_security,
)
from packager.change_over_time.scripts.convert_quality_of_life import (
    convert_quality_of_life,
)


def run_all_conversions():
    """Run all change-over-time conversions for all categories and geographies."""
    # Define all categories and geographies to process
    categories_with_old_new = ["demographics", "economics"]
    categories_single_file = ["housing_security", "quality_of_life"]
    geographies = ["borough", "puma", "citywide"]

    print("=" * 80)
    print("EDDE Change-Over-Time Conversion")
    print("=" * 80)
    print()

    # Get base paths and yearbands
    try:
        old_edde_path, new_build_path = get_edde_paths()
        old_yearband, new_yearband = get_yearbands()

        # Output to attachments/change_over_time directory (similar to site_conf)
        # new_build_path is .../build_name/dataset_files, so parent is .../build_name/
        # (where build_name is the partition key like "2026:ar_edde:20260623T1809")
        output_dir = new_build_path.parent / "attachments" / "change_over_time"
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"Old EDDE data: {old_edde_path}")
        print(f"New build data: {new_build_path}")
        print(f"Output directory: {output_dir}")
        print(f"Yearbands: {old_yearband} -> {new_yearband}")
        print()
    except Exception as e:
        print(f"Error: Failed to get EDDE paths: {e}")
        raise RuntimeError(f"Failed to get EDDE paths: {e}")

    # Track success/failure
    total = 0
    successful = 0
    failed = []

    # Process demographics and economics (require old + new files)
    for category in categories_with_old_new:
        converter_func = (
            convert_demographics if category == "demographics" else convert_economics
        )

        for geography in geographies:
            total += 1
            try:
                print(f"Processing {category} - {geography}...")

                old_csv = get_old_csv(category, geography)
                new_csv = get_new_csv(category, geography)

                # Create category subdirectory
                category_dir = output_dir / category
                category_dir.mkdir(parents=True, exist_ok=True)
                output_csv = category_dir / f"{category}_change_{geography}.csv"

                converter_func(old_csv, new_csv, output_csv, geography)
                successful += 1
                print(f"  ✓ Success: {output_csv.name}")
                print()
            except Exception as e:
                failed.append((category, geography, str(e)))
                print(f"  ✗ Failed: {e}")
                print()

    # Process housing_security and quality_of_life (single file with multiple yearbands)
    for category in categories_single_file:
        converter_func = (
            convert_housing_security
            if category == "housing_security"
            else convert_quality_of_life
        )

        for geography in geographies:
            total += 1
            try:
                print(f"Processing {category} - {geography}...")

                input_csv = get_new_csv(category, geography)

                # Create category subdirectory
                category_dir = output_dir / category
                category_dir.mkdir(parents=True, exist_ok=True)
                output_csv = category_dir / f"{category}_change_{geography}.csv"

                # Pass yearbands to converter
                converter_func(
                    input_csv, output_csv, geography, old_yearband, new_yearband
                )
                successful += 1
                print(f"  ✓ Success: {output_csv.name}")
                print()
            except Exception as e:
                failed.append((category, geography, str(e)))
                print(f"  ✗ Failed: {e}")
                print()

    # Print summary
    print("=" * 80)
    print(f"Summary: {successful}/{total} conversions successful")
    print("=" * 80)

    if failed:
        print()
        print("Failed conversions:")
        for category, geography, error in failed:
            print(f"  - {category}/{geography}: {error}")
        raise RuntimeError(f"{len(failed)} conversion(s) failed")
    else:
        print("\n✓ All conversions completed successfully!")
