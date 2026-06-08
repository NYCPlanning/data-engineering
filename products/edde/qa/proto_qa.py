"""QA script to compare previous and current versions of EDDE builds.

Compares all indicator CSVs across two build versions to identify differences.
"""

from pathlib import Path

import pandas as pd

from dcpy.lifecycle.builds import get_recipe_lock
from dcpy.utils.logging import logger

# PUMA to use for focused analysis (Park Slope)
PARK_SLOPE_PUMA = "04306"

# Build paths to compare
# TODO: we need to integrate this into the build a little better.
OLD_BUILD_PATH = Path(
    "/Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/eddt-2025/data"
)
NEW_BUILD_PATH = Path(
    "/Users/alexrichey/dev/.de_lifecycle_data/builds/build/edde/eddt-2026/data"
)

assert OLD_BUILD_PATH
assert NEW_BUILD_PATH

GEOS = ["citywide", "borough", "puma"]
CATEGORIES = [
    "demographics",
    "economics",
    "housing_security",
    "housing_production",
    "quality_of_life",
]

# Year band mappings for normalization
OLD_YEAR_BANDS = {"0812": "prev", "1923": "current"}
NEW_YEAR_BANDS = {"0812": "prev", "2024": "current"}

# Product path for loading recipe lock
PRODUCT_PATH = Path(__file__).parent.parent

# QA output directory (relative to build output)
QA_OUTPUT_SUBDIR = "qa"


def decompose_column_name(col_name: str) -> dict[str, str]:
    """Decompose a column name into its constituent parts.

    This uses EDDE's naming conventions to extract:
    - base_variable: The core indicator (e.g., 'access_households', 'units_occupied', 'health_diabetes')
    - year_band: The ACS period (e.g., '0812', '1923', '2024', 'prev', 'current')
    - race: Race/ethnicity code (e.g., 'wnh', 'bnh', 'hsp', 'anh')
    - measure: The type of measurement (e.g., 'count', 'pct', 'count_moe', 'pct_moe', 'median')

    Args:
        col_name: Column name to decompose

    Returns:
        Dictionary with keys: base_variable, year_band, race, measure
    """
    parts = col_name.split("_")

    # Common race codes
    race_codes = {"wnh", "bnh", "hsp", "anh"}
    # Common measure suffixes
    measures = {
        "count",
        "pct",
        "median",
        "cv",
        "lower",
        "upper",
        "moe",
        "total",
    }
    # Year band patterns (4 digit years or semantic names)
    year_patterns = {"0812", "1923", "2024", "0913", "prev", "current"}

    result = {"base_variable": "", "year_band": "", "race": "", "measure": ""}

    # Work backwards from the end to identify measure
    measure_parts = []
    i = len(parts) - 1
    while i >= 0 and parts[i] in measures:
        measure_parts.insert(0, parts[i])
        i -= 1

    if measure_parts:
        result["measure"] = "_".join(measure_parts)

    # Check if there's a race code before the measure
    if i >= 0 and parts[i] in race_codes:
        result["race"] = parts[i]
        i -= 1

    # Check if there's a year band before the race (or before measure if no race)
    if i >= 0 and parts[i] in year_patterns:
        result["year_band"] = parts[i]
        i -= 1

    # Everything else is the base variable
    if i >= 0:
        result["base_variable"] = "_".join(parts[: i + 1])

    return result


def get_ignored_indicators() -> set[str]:
    """Get list of ignored indicators from current recipe lock file."""
    try:
        recipe_lock = get_recipe_lock(PRODUCT_PATH)
        if recipe_lock.custom and "ignored_indicators" in recipe_lock.custom:
            ignored = set(recipe_lock.custom["ignored_indicators"])
            logger.info(f"Loaded ignored indicators: {ignored}")
            return ignored
    except Exception as e:
        logger.warning(f"Could not load ignored indicators: {e}")
    return set()


def normalize_variable_name(var_name: str, year_band_map: dict[str, str]) -> str:
    """Normalize variable name by replacing year bands with semantic names.

    Args:
        var_name: Original variable name
        year_band_map: Mapping of year bands to normalized names

    Returns:
        Normalized variable name with year bands replaced
    """
    normalized = var_name
    for year_band, semantic_name in year_band_map.items():
        normalized = normalized.replace(f"_{year_band}_", f"_{semantic_name}_")
    return normalized


def all_indicators(
    build_path: Path, version: str, year_band_map: dict[str, str]
) -> pd.DataFrame:
    """Read in all indicator files into a single dataframe from a single build.

    Args:
        build_path: Path to the build directory
        version: Version label (e.g., "2025", "2026")
        year_band_map: Mapping of year bands to normalized names

    Returns:
        DataFrame with all indicators combined
    """
    logger.info(f"Loading indicators from {build_path} (version: {version})")

    if not build_path.exists():
        raise FileNotFoundError(f"Build path does not exist: {build_path}")

    combined_df = None

    for category in CATEGORIES:
        logger.info(f"  Processing category: {category}")
        category_path = build_path / category

        if not category_path.exists():
            logger.warning(f"    Category path does not exist: {category_path}")
            continue

        csv_files = [f for f in category_path.iterdir() if f.suffix == ".csv"]
        logger.info(f"    Found {len(csv_files)} CSV files")

        for file in csv_files:
            # Parse filename: {category}_{year}_{geo}.csv or {category}_{geo}.csv
            ind_csv_parts = (
                file.name.replace(f"{category}_", "").replace(".csv", "").split("_")
            )
            year = geo = None
            if len(ind_csv_parts) == 2:
                year, geo = ind_csv_parts
            else:
                geo = ind_csv_parts[0]

            logger.info(f"      Loading {file.name} (geo={geo}, year={year})")

            df = pd.read_csv(file, dtype={geo: str})
            df = df.melt(geo).rename(columns={geo: "geo"})
            df["geo2"] = df.geo
            df["category"] = category
            df["geo_type"] = geo
            df["version"] = version
            df["census_year"] = year or ""
            df["full_variable"] = df["variable"] + df["census_year"]
            # Normalize variable names for comparison
            df["normalized_variable"] = df["variable"].apply(
                lambda x: normalize_variable_name(x, year_band_map)
            )

            if combined_df is not None:
                combined_df = pd.concat([combined_df, df])
            else:
                combined_df = df

    if combined_df is None:
        raise ValueError(f"No data found in build path: {build_path}")

    logger.info(f"Loaded {len(combined_df)} total rows for version {version}")
    return combined_df


def compare_builds():
    """Compare two EDDE builds and output differences."""
    logger.info("Starting EDDE build comparison")
    logger.info(f"Old build: {OLD_BUILD_PATH}")
    logger.info(f"New build: {NEW_BUILD_PATH}")
    logger.info(f"Old year bands: {OLD_YEAR_BANDS}")
    logger.info(f"New year bands: {NEW_YEAR_BANDS}")

    # Load ignored indicators from current recipe lock
    ignored_indicators = get_ignored_indicators()

    # Load both builds with year band normalization
    old_ind = all_indicators(OLD_BUILD_PATH, "2025", OLD_YEAR_BANDS)
    new_ind = all_indicators(NEW_BUILD_PATH, "2026", NEW_YEAR_BANDS)

    logger.info(f"\nTotal rows in 2025 build: {len(old_ind)}")
    logger.info(f"Total rows in 2026 build: {len(new_ind)}")

    # Combine and index using normalized_variable for comparison
    all_together = pd.concat([old_ind, new_ind])
    indexed = all_together.set_index(
        ["geo_type", "category", "normalized_variable", "geo", "version"]
    ).sort_index()

    logger.info(
        f"\nUnique geo types: {indexed.index.get_level_values('geo_type').unique().tolist()}"
    )
    logger.info(
        f"Unique categories: {indexed.index.get_level_values('category').unique().tolist()}"
    )
    logger.info(
        f"Unique versions: {indexed.index.get_level_values('version').unique().tolist()}"
    )

    # Pivot to compare values across versions
    comparison = indexed.pivot_table(
        index=["geo_type", "category", "normalized_variable", "geo"],
        columns="version",
        values="value",
        aggfunc="first",
    )

    # Calculate differences
    comparison["diff"] = comparison["2026"] - comparison["2025"]

    # Calculate percent change with special handling for appeared/disappeared values
    # - If 2025 is null and 2026 has value: +100% (appeared)
    # - If 2025 has value and 2026 is null: -100% (disappeared)
    # - Otherwise: normal percent change calculation
    comparison["pct_change"] = (
        (comparison["2026"] - comparison["2025"]) / comparison["2025"] * 100
    )
    # Set appeared values (null -> value) to +100%
    appeared_mask = comparison["2025"].isna() & comparison["2026"].notna()
    comparison.loc[appeared_mask, "pct_change"] = 100.0
    # Set disappeared values (value -> null) to -100%
    disappeared_mask = comparison["2025"].notna() & comparison["2026"].isna()
    comparison.loc[disappeared_mask, "pct_change"] = -100.0

    # Filter to rows with differences OR where one version has null and the other doesn't
    # This captures:
    # 1. Normal changes: diff != 0 and diff is not null
    # 2. Appeared: 2025 is null, 2026 has value
    # 3. Disappeared: 2025 has value, 2026 is null
    has_change = comparison["diff"].notna() & (comparison["diff"] != 0)
    appeared = comparison["2025"].isna() & comparison["2026"].notna()
    disappeared = comparison["2025"].notna() & comparison["2026"].isna()

    differences = comparison[has_change | appeared | disappeared]

    logger.info(f"\nFound {len(differences)} rows with differences")
    logger.info(f"  Normal changes: {has_change.sum()}")
    logger.info(f"  Appeared (was null, now has value): {appeared.sum()}")
    logger.info(f"  Disappeared (had value, now null): {disappeared.sum()}")

    if len(differences) > 0:
        # Show summary statistics
        logger.info("\nDifference statistics:")
        logger.info(f"  Mean difference: {differences['diff'].mean():.2f}")
        logger.info(f"  Median difference: {differences['diff'].median():.2f}")
        logger.info(f"  Max absolute difference: {differences['diff'].abs().max():.2f}")

        # Add absolute difference column for sorting
        differences["abs_diff"] = differences["diff"].abs()

        # Add needs_review flag for changes >50% (absolute value)
        differences["needs_review"] = differences["pct_change"].abs() > 50

        # Sort by needs_review (True first), then by absolute difference (largest to smallest)
        differences_sorted = differences.sort_values(
            ["needs_review", "abs_diff"], ascending=[False, False]
        )

        # Reset index to get normalized_variable as a column
        differences_sorted = differences_sorted.reset_index()
        # Rename normalized_variable to variable for clarity
        differences_sorted = differences_sorted.rename(
            columns={"normalized_variable": "variable"}
        )

        # Add base_variable column by decomposing the variable name
        differences_sorted["base_variable"] = differences_sorted["variable"].apply(
            lambda v: decompose_column_name(v)["base_variable"]
        )

        # Mark ignored indicators as not needing review
        for ignored_indicator in ignored_indicators:
            mask = differences_sorted["base_variable"] == ignored_indicator
            differences_sorted.loc[mask, "needs_review"] = False
            if mask.sum() > 0:
                logger.info(
                    f"Marked {mask.sum()} rows as not needing review (ignored indicator: {ignored_indicator})"
                )

        # Report on needs_review statistics
        needs_review_count = differences_sorted["needs_review"].sum()
        logger.info(
            f"\nNeeds review: {needs_review_count} rows "
            f"({needs_review_count / len(differences_sorted) * 100:.1f}%)"
        )

        # Determine output directory - use NEW_BUILD_PATH's parent qa/ folder
        # NEW_BUILD_PATH is {build_dir}/data, so parent is {build_dir}
        qa_output_dir = NEW_BUILD_PATH.parent / QA_OUTPUT_SUBDIR
        qa_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"QA output directory: {qa_output_dir}")

        output_file = qa_output_dir / "edde_build_comparison.csv"
        differences_sorted.to_csv(output_file, index=False)
        logger.info(f"\nFull comparison saved to: {output_file}")
        logger.info(f"Total differences: {len(differences_sorted)} rows")
        logger.info(
            "Sorted by needs_review (True first), then absolute difference (largest to smallest)"
        )

        # Create aggregated summary by category and indicator
        summary = (
            differences_sorted.groupby(["category", "base_variable"])
            .agg(
                count_needs_review=("needs_review", lambda x: x.sum()),
                count_ok=("needs_review", lambda x: (~x).sum()),
            )
            .reset_index()
        )
        summary["total"] = summary["count_needs_review"] + summary["count_ok"]
        summary["pct"] = (summary["count_needs_review"] / summary["total"] * 100).round(
            1
        )
        summary = summary.rename(columns={"base_variable": "indicator"})
        summary = summary[
            ["category", "indicator", "count_ok", "count_needs_review", "pct"]
        ]

        # Sort by category, then by pct (descending), then by indicator
        summary = summary.sort_values(
            ["category", "pct", "indicator"], ascending=[True, False, True]
        )

        summary_file = qa_output_dir / "edde_build_comparison_summary.csv"
        summary.to_csv(summary_file, index=False)
        logger.info(f"\nAggregated summary saved to: {summary_file}")
        logger.info(f"Total indicators with changes: {len(summary)}")

        # Identify unchanged indicators (no differences in any geography)
        logger.info("\nIdentifying unchanged indicators...")

        # Add base_variable to comparison for grouping
        comparison_with_base = comparison.reset_index()
        comparison_with_base["base_variable"] = comparison_with_base[
            "normalized_variable"
        ].apply(lambda v: decompose_column_name(v)["base_variable"])

        # Group by category + base_variable and check if ANY row has a difference
        # An indicator is unchanged if all its rows have no difference
        indicator_has_change = comparison_with_base.groupby(
            ["category", "base_variable"]
        ).apply(
            lambda group: (
                (group["diff"].notna() & (group["diff"] != 0))
                | (group["2025"].isna() & group["2026"].notna())
                | (group["2025"].notna() & group["2026"].isna())
            ).any()
        )

        # Filter to indicators with NO changes
        unchanged_indicators = indicator_has_change[~indicator_has_change].reset_index()
        unchanged_indicators = unchanged_indicators.rename(
            columns={"base_variable": "indicator"}
        )
        unchanged_indicators = unchanged_indicators[
            ["category", "indicator"]
        ].sort_values(["category", "indicator"])

        unchanged_file = qa_output_dir / "edde_unchanged_indicators.csv"
        unchanged_indicators.to_csv(unchanged_file, index=False)
        logger.info(f"\nUnchanged indicators saved to: {unchanged_file}")
        logger.info(f"Total unchanged indicators: {len(unchanged_indicators)}")

        # Show breakdown by category
        unchanged_by_category = unchanged_indicators.groupby("category").size()
        logger.info("\nUnchanged indicators by category:")
        for category, count in unchanged_by_category.items():
            logger.info(f"  {category}: {count}")

    return indexed, comparison, differences


if __name__ == "__main__":
    indexed, comparison, differences = compare_builds()
