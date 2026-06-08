"""
Core change-over-time calculator matching legacy JavaScript behavior.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


class ChangeCalculator:
    """Calculate change-over-time statistics matching legacy JS behavior."""

    # Top/bottom codes for median values (from JS lines 27-65)
    RENT_MEDIAN_NEW_TOPCODE = 3500
    RENT_MEDIAN_OLD_TOPCODE = 2300
    HOMEVALUE_MEDIAN_NEW_TOPCODE = 2000000
    HOMEVALUE_MEDIAN_OLD_TOPCODE = 1182000

    def __init__(
        self,
        input_csv: Path,
        labels: List[str],
        geography_col: str,
        old_yearband: str = "0812",
        new_yearband: str = "1923",
        special_yearbands: Optional[Dict[str, Tuple[str, str]]] = None,
        decimal_change_labels: Optional[List[str]] = None,
        strip_suffix_for_derived: bool = False,
        top_bottom_codes: Optional[Dict[str, List[int]]] = None,
    ):
        """
        Initialize change calculator.

        Args:
            input_csv: Path to input CSV with multiple year bands
            labels: Base variable names to process (e.g., 'dhs_shelter_change_count')
            geography_col: Name of geography column ('puma', 'borough', 'citywide')
            old_yearband: Default old year band (e.g., '0812')
            new_yearband: Default new year band (e.g., '1923')
            special_yearbands: Dict mapping variable prefixes to (old, new) tuples
                              e.g., {'dhs_shelter': ('2020', '2022')}
            decimal_change_labels: Labels that should use 1 decimal place for change value
                                  instead of rounding to 0 (e.g., age_median variables)
            strip_suffix_for_derived: If True, strip type suffix (_count, _median) from derived
                                     column names (_pct, _pnt). Used by demographics.
            top_bottom_codes: Dict with 'old' and 'new' keys containing lists of top/bottom codes
                             that should skip calculations (e.g., {'old': [223000, 11000], 'new': [200000, 9999]})
        """
        self.input_csv = Path(input_csv)
        self.labels = labels
        self.geography_col = geography_col
        self.old_yearband = old_yearband
        self.new_yearband = new_yearband
        self.special_yearbands = special_yearbands or {}
        self.decimal_change_labels = set(decimal_change_labels or [])
        self.strip_suffix_for_derived = strip_suffix_for_derived
        self.top_bottom_codes = top_bottom_codes or {}

    def _get_yearband_for_variable(self, variable: str) -> Tuple[str, str]:
        """Get (old, new) yearband tuple for a given variable."""
        # Check if this variable has special yearband mapping
        for prefix, (old, new) in self.special_yearbands.items():
            if variable.startswith(prefix):
                return (old, new)
        return (self.old_yearband, self.new_yearband)

    def _extract_dataset(
        self, df: pd.DataFrame, yearband: str, is_old: bool
    ) -> pd.DataFrame:
        """
        Extract a dataset for a specific yearband, renaming columns to _change_ format.

        Args:
            df: Input dataframe with year-banded columns
            yearband: Year band to extract (e.g., '0812', '1923')
            is_old: Whether this is the old dataset (affects which variables to include)

        Returns:
            DataFrame with columns renamed to {variable}_change_{type} format
        """
        result_data = {self.geography_col: df[self.geography_col]}

        for label in self.labels:
            # Extract base variable name (remove _change_ suffix)
            # label format: "{variable}_change_{type}"
            # e.g., "dhs_shelter_change_count" -> variable="dhs_shelter", type="count"
            parts = label.split("_change_")
            if len(parts) != 2:
                raise ValueError(
                    f"Invalid label format: {label}. Expected {{variable}}_change_{{type}}"
                )

            variable_prefix = parts[0]
            suffix = parts[1]  # e.g., "count", "median", "anh_count", etc.

            # Determine which yearband to use for this variable
            old_yb, new_yb = self._get_yearband_for_variable(variable_prefix)
            use_yearband = old_yb if is_old else new_yb

            # Construct the input column name
            # Input format: "{variable}_{yearband}_{type}"
            # Need to handle both simple suffixes (count, median) and subgroup suffixes (anh_count)
            input_col_base = f"{variable_prefix}_{use_yearband}_{suffix}"

            # Copy the main column
            if input_col_base in df.columns:
                result_data[label] = df[input_col_base]
            else:
                # Column doesn't exist - set to NaN
                result_data[label] = np.nan

            # Copy companion columns (_moe, _cv)
            for companion_suffix in ["_moe", "_cv"]:
                input_companion = f"{input_col_base}{companion_suffix}"
                output_companion = f"{label}{companion_suffix}"

                if input_companion in df.columns:
                    result_data[output_companion] = df[input_companion]
                else:
                    result_data[output_companion] = np.nan

            # Also copy the base variable's _pct and _pct_moe columns
            # e.g., for label "units_occupied_owner_change_count":
            #   copy units_occupied_owner_0812_pct -> units_occupied_owner_change_pct
            # e.g., for label "units_occupied_owner_change_anh_count":
            #   copy units_occupied_owner_0812_anh_pct -> units_occupied_owner_change_anh_pct
            # This is needed for percentage point calculations

            # Strip the type suffix (count/median) but keep subgroup prefix
            # suffix could be: "count", "median", "anh_count", "anh_median", etc.
            if suffix.endswith("_count") or suffix.endswith("_median"):
                # Has subgroup prefix (e.g., "anh_count" -> keep "anh")
                base_suffix = suffix.rsplit("_", 1)[0]  # "anh_count" -> "anh"
                base_var = f"{variable_prefix}_change_{base_suffix}"
                base_input = f"{variable_prefix}_{use_yearband}_{base_suffix}"
            else:
                # No subgroup prefix (e.g., "count" or "median")
                base_var = f"{variable_prefix}_change"
                base_input = f"{variable_prefix}_{use_yearband}"

            for pct_suffix in ["_pct", "_pct_moe"]:
                input_pct = f"{base_input}{pct_suffix}"
                output_pct = f"{base_var}{pct_suffix}"

                if input_pct in df.columns:
                    result_data[output_pct] = df[input_pct]
                else:
                    result_data[output_pct] = np.nan

        return pd.DataFrame(result_data)

    def create_datasets(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Extract OLD and NEW datasets from input CSV.

        Returns:
            (old_df, new_df) tuple of DataFrames
        """
        # Read input CSV
        df = pd.read_csv(
            self.input_csv,
            dtype={self.geography_col: str},  # Keep geography as string
        )

        old_df = self._extract_dataset(df, self.old_yearband, is_old=True)
        new_df = self._extract_dataset(df, self.new_yearband, is_old=False)

        return old_df, new_df

    def _format_decimal(self, value: float, decimals: int = 1) -> any:
        """
        Format a numeric value, returning int if whole number, float otherwise.
        Matches JS behavior where .toFixed() can produce integers or decimals.
        """
        rounded = float(f"{value:.{decimals}f}")
        # Return integer if it's a whole number
        if rounded == int(rounded):
            return int(rounded)
        return rounded

    def _get_base_label(self, label: str) -> str:
        """
        Get the base label without type suffix (_count, _median, _rate).
        Used for creating derived columns like _pct and _pnt.

        Examples:
            "pop_change_count" -> "pop_change"
            "age_median_change_anh_median" -> "age_median_change_anh"
            "health_infantmortality_change_rate" -> "health_infantmortality_change"
        """
        for suffix in ["_median", "_count", "_rate"]:
            if label.endswith(suffix):
                return label[: -len(suffix)]
        return label

    def _should_skip_calculation(
        self, label: str, old_val: float, new_val: float
    ) -> bool:
        """
        Check if we should skip calculations due to top/bottom coding.
        Matches JS lines 68-71 for housing_security.
        Matches JS lines 62-70 for economics.
        """
        # Housing security: rent_median and homevalue_median special cases
        rent_median_labels = [
            "rent_median_change_median",
            "rent_median_change_anh_median",
            "rent_median_change_bnh_median",
            "rent_median_change_hsp_median",
            "rent_median_change_wnh_median",
        ]
        homevalue_median_labels = [
            "homevalue_median_change_median",
            "homevalue_median_change_anh_median",
            "homevalue_median_change_bnh_median",
            "homevalue_median_change_hsp_median",
            "homevalue_median_change_wnh_median",
        ]

        if label in rent_median_labels:
            if new_val == self.RENT_MEDIAN_NEW_TOPCODE:
                return True
            if old_val == self.RENT_MEDIAN_OLD_TOPCODE:
                return True

        if label in homevalue_median_labels:
            if new_val == self.HOMEVALUE_MEDIAN_NEW_TOPCODE:
                return True
            if old_val == self.HOMEVALUE_MEDIAN_OLD_TOPCODE:
                return True

        # Economics: household_income and occupation/industry wages medians
        # Check if this is a median variable AND if configurable top/bottom codes provided
        if label.endswith("_median") and self.top_bottom_codes:
            old_codes = self.top_bottom_codes.get("old", [])
            new_codes = self.top_bottom_codes.get("new", [])

            # Check if value matches any of the top/bottom codes
            if old_val in old_codes or new_val in new_codes:
                return True

        return False

    def _calculate_pct_change(self, old_val: float, new_val: float) -> any:
        """
        Calculate percent change. Matches JS lines 100-109.

        Returns:
            String with 1 decimal place, or "" for blank, or numeric 0/-100
        """
        if pd.isna(old_val) or pd.isna(new_val):
            return ""

        if old_val == 0 and new_val > 0:
            return 0
        elif old_val > 0 and new_val == 0:
            return -100
        elif old_val == 0 and new_val == 0:
            return 0
        elif old_val != 0:
            result = ((new_val - old_val) / old_val) * 100
            return self._format_decimal(result, 1)
        else:
            return ""

    def _calculate_pct_change_moe(
        self,
        old_val: float,
        new_val: float,
        old_moe: float,
        new_moe: float,
        pct_change: any,
    ) -> any:
        """
        Calculate percent change MOE. Matches JS lines 117-128.
        """
        if pd.isna(old_val) or pd.isna(new_val) or pd.isna(old_moe) or pd.isna(new_moe):
            return ""

        if old_val == 0 and new_val > 0:
            return ""
        elif old_val > 0 and new_val == 0:
            return 0
        elif old_val == 0 and new_val == 0:
            return ""
        elif old_val != 0 and pct_change != 100:
            # Formula: ((SQRT(((new_moe)^2)+(((new_val/old_val)^2)*((old_moe)^2)))/old_val)*100)
            result = (
                np.sqrt((new_moe**2) + (((new_val / old_val) ** 2) * (old_moe**2)))
                / old_val
            ) * 100
            return self._format_decimal(result, 1)
        else:
            return ""

    def _calculate_pnt_change(self, old_pct: float, new_pct: float) -> any:
        """
        Calculate percentage point change. Matches JS lines 143-148.
        """
        if pd.isna(old_pct) or pd.isna(new_pct):
            return ""

        if (old_pct + new_pct) > 0:
            result = new_pct - old_pct
            return self._format_decimal(result, 1)
        else:
            return ""

    def _calculate_pnt_change_moe(
        self, old_pct_moe: float, new_pct_moe: float, pnt_change: any
    ) -> any:
        """
        Calculate percentage point change MOE. Matches JS lines 164-168.
        """
        # If pnt_change is blank/None, return blank
        if pnt_change == "" or pnt_change is None:
            return ""

        # Try to calculate (will produce NaN if inputs are NaN, matching JS behavior)
        # Formula: SQRT((new_pct_moe^2) + (old_pct_moe^2))
        # When MOE values are NaN, this produces NaN which gets cleaned up later
        result = np.sqrt((new_pct_moe**2) + (old_pct_moe**2))

        # If result is NaN, return blank (will be cleaned up by final NaN conversion)
        if pd.isna(result):
            return np.nan

        return self._format_decimal(result, 1)

    def _calculate_change_for_variable(
        self, label: str, old_row: pd.Series, new_row: pd.Series
    ) -> Dict:
        """
        Calculate change statistics for a single variable.
        Matches logic from lines 75-172 of 2025-convert-housing_security.js
        """
        result = {}

        # Check if we should skip this calculation (top/bottom coding)
        skip = self._should_skip_calculation(
            label, old_row.get(label, np.nan), new_row.get(label, np.nan)
        )

        if not skip:
            old_val = old_row.get(label, np.nan)
            new_val = new_row.get(label, np.nan)
            old_moe = old_row.get(f"{label}_moe", np.nan)
            new_moe = new_row.get(f"{label}_moe", np.nan)

            # Change (line 84) - round to 0 decimal places (or 1 for special labels like age_median)
            if not pd.isna(old_val) and not pd.isna(new_val):
                change_val = new_val - old_val
                if label in self.decimal_change_labels:
                    # Special case: age_median uses 1 decimal place (see convert-demo.js line 77)
                    result[label] = self._format_decimal(change_val, 1)
                else:
                    result[label] = round(change_val)
            else:
                result[label] = ""

            # Change MOE (line 91) - round to 0 decimal places
            if not pd.isna(old_moe) and not pd.isna(new_moe):
                result[f"{label}_moe"] = round(
                    abs(np.sqrt((new_moe**2) + (old_moe**2)))
                )
            else:
                result[f"{label}_moe"] = ""

            # Determine output column names for derived columns
            # Demographics strips suffix (_median, _count) from derived column names
            # Housing security and others keep the suffix
            if self.strip_suffix_for_derived:
                pct_label = self._get_base_label(label)
                pnt_label = self._get_base_label(label)
            else:
                pct_label = label
                pnt_label = label

            # Percent change (lines 100-109)
            result[f"{pct_label}_pct"] = self._calculate_pct_change(old_val, new_val)

            # Percent change MOE (lines 117-128)
            # Only add this column if MOE values exist (line 123: if(pumaNew[`${thisLabel}_moe`]))
            if not pd.isna(new_moe):
                result[f"{pct_label}_pct_moe"] = self._calculate_pct_change_moe(
                    old_val, new_val, old_moe, new_moe, result[f"{pct_label}_pct"]
                )

            # Percentage point change (lines 143-148)
            # Always strip type suffix to look up _pct values
            base_label = self._get_base_label(label)
            old_pct = old_row.get(f"{base_label}_pct", np.nan)
            new_pct = new_row.get(f"{base_label}_pct", np.nan)

            result[f"{pnt_label}_pnt"] = self._calculate_pnt_change(old_pct, new_pct)

            # Percentage point change MOE (lines 164-168)
            old_pct_moe = old_row.get(f"{base_label}_pct_moe", np.nan)
            new_pct_moe = new_row.get(f"{base_label}_pct_moe", np.nan)

            result[f"{pnt_label}_pnt_moe"] = self._calculate_pnt_change_moe(
                old_pct_moe, new_pct_moe, result[f"{pnt_label}_pnt"]
            )

        return result

    def calculate_change(
        self, old_df: pd.DataFrame, new_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Apply change calculations matching JS logic.
        Matches JS calculateChange function (lines 15-182).
        """
        change_data = []

        for idx in range(len(old_df)):
            old_row = old_df.iloc[idx]
            new_row = new_df.iloc[idx]

            row_result = {self.geography_col: old_row[self.geography_col]}

            # Process each label
            for label in self.labels:
                var_result = self._calculate_change_for_variable(
                    label, old_row, new_row
                )
                row_result.update(var_result)

            # Convert NaN to empty string (lines 174-176)
            for key, value in row_result.items():
                if pd.isna(value):
                    row_result[key] = ""

            change_data.append(row_result)

        return pd.DataFrame(change_data)

    def run(self) -> pd.DataFrame:
        """
        Full pipeline: load -> extract -> calculate -> return.

        Returns:
            DataFrame with change statistics
        """
        old_df, new_df = self.create_datasets()
        result_df = self.calculate_change(old_df, new_df)
        return result_df
