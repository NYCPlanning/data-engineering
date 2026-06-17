"""Helper functions for BPL libraries ingest templates.

This module provides validation and processing functions for the BPL libraries dataset
and its downstream dependencies.
"""

import pandas as pd


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that the BPL libraries dataset has required columns.

    This is a simple validation that checks for the presence of the 'title' column.
    If the column is missing, raises a ValueError which will fail the ingest.

    Args:
        df: Input DataFrame from bpl_libraries dataset

    Returns:
        A simple DataFrame with validation results (single row indicating success)

    Raises:
        ValueError: If required columns are missing
    """
    # TODO: TEMPORARY - Remove this after testing Dagster failure handling
    raise ValueError(
        "Validation intentionally failing for Dagster testing. "
        "This tests that downstream assets (bpl_libraries_supplemented) are blocked when validation fails."
    )

    required_columns = ["title"]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Validation failed: Required columns {missing_columns} not found in dataset. "
            f"Available columns: {list(df.columns)}"
        )

    # Return a simple validation result DataFrame
    # This could be expanded to include more validation metadata
    return pd.DataFrame(
        {
            "validation_status": ["PASSED"],
            "rows_validated": [len(df)],
            "columns_checked": [", ".join(required_columns)],
        }
    )


def add_random_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add a random number column to the dataset.

    This demonstrates downstream processing that depends on validation.
    Adds a 'random_value' column with random numbers between 0 and 100.

    Args:
        df: Input DataFrame from bpl_libraries dataset

    Returns:
        DataFrame with additional 'random_value' column
    """
    import numpy as np

    result = df.copy()
    result["random_value"] = np.random.randint(0, 100, size=len(df))

    return result
