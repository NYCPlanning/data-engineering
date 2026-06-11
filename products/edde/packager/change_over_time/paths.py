"""Helper module for resolving EDDE data paths for change-over-time calculations."""

import json
from pathlib import Path

from config import get_edde_paths

from dcpy.lifecycle.builds import get_build_metadata_path

PRODUCT_PATH = Path(__file__).parent.parent.parent


def get_yearbands() -> tuple[str, str]:
    """Get old and new yearbands from build_metadata.json.

    Returns:
        Tuple of (old_yearband, new_yearband) from recipe vars
    """
    build_metadata_path = get_build_metadata_path(PRODUCT_PATH)
    with open(build_metadata_path, "r") as f:
        build_metadata = json.load(f)

    vars = build_metadata["recipe"]["vars"]
    old_yearband = vars.get("BUILD_ENV_EDDE_ACS_PREV_YEAR_BAND", "0812")
    new_yearband = vars.get("BUILD_ENV_EDDE_ACS_CURRENT_YEAR_BAND", "2024")

    return old_yearband, new_yearband


def get_old_csv(category: str, geography: str) -> Path:
    """Get path to old (previous EDDE version) CSV for a category and geography.

    Args:
        category: Category name (e.g., 'demographics', 'economics')
        geography: Geography type (e.g., 'borough', 'puma', 'citywide')

    Returns:
        Path to the old CSV file

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    old_edde_path, _ = get_edde_paths()

    # For demographics and economics, the old files come from previous EDDE
    # File naming: {category}_{yearband}_{geography}.csv
    # But we need to find which yearband files exist
    category_dir = old_edde_path / category
    if not category_dir.exists():
        raise FileNotFoundError(
            f"Category directory not found: {category_dir}. "
            f"Expected to find old EDDE data for {category}."
        )

    # Look for CSV files matching the pattern
    # For demographics/economics, files are like: demographics_0812_borough.csv
    matching_files = list(category_dir.glob(f"{category}_*_{geography}.csv"))

    if not matching_files:
        raise FileNotFoundError(
            f"No CSV files found for {category}/{geography} in {category_dir}"
        )

    # If multiple files, prefer the one with the oldest yearband (e.g., 0812 vs 1923)
    # Sort alphabetically - '0812' will come before '1923'
    matching_files.sort()
    old_csv = matching_files[0]

    return old_csv


def get_new_csv(category: str, geography: str) -> Path:
    """Get path to new (current build) CSV for a category and geography.

    Args:
        category: Category name (e.g., 'demographics', 'economics', 'housing_security', 'quality_of_life')
        geography: Geography type (e.g., 'borough', 'puma', 'citywide')

    Returns:
        Path to the new CSV file

    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    _, new_build_path = get_edde_paths()

    category_dir = new_build_path / category
    if not category_dir.exists():
        raise FileNotFoundError(
            f"Category directory not found: {category_dir}. "
            f"Expected to find current build data for {category}."
        )

    # For demographics/economics with separate old/new files:
    #   - Use the current yearband from recipe vars (e.g., demographics_2024_borough.csv)
    # For housing_security/quality_of_life with single file:
    #   - Look for the file without yearband (e.g., housing_security_borough.csv)

    if category in ["demographics", "economics"]:
        # Use current yearband from recipe vars
        _, new_yearband = get_yearbands()
        new_csv = category_dir / f"{category}_{new_yearband}_{geography}.csv"
        if not new_csv.exists():
            raise FileNotFoundError(
                f"File not found: {new_csv}\n"
                f"Expected current yearband file for {category}/{geography}"
            )
    else:
        # housing_security or quality_of_life - single file without yearband
        new_csv = category_dir / f"{category}_{geography}.csv"
        if not new_csv.exists():
            raise FileNotFoundError(f"File not found: {new_csv}")

    return new_csv
