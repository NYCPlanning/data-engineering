import pandas as pd
from pathlib import Path
import yaml


def validate_csv(csv_path: Path, metadata_path: Path):
    """Validate a csv against a metadata file."""
    csv_df = pd.read_csv(csv_path, dtype=str)
    csv_df_non_null = csv_df.fillna("")

    metadata = yaml.safe_load(open(metadata_path, "r"))

    errors = []

    # Find mismatched columns
    # Assumes that if `appears_in` isn't set, every filetype will contain the column
    metadata_column_names = {
        x["name"]
        for x in metadata["columns"]
        if not x.get("appears_in") or "csv" in x.get("appears_in")
    }
    csv_headers = set(csv_df.columns)

    extras_in_csv = csv_headers.difference(metadata_column_names)
    if extras_in_csv:
        errors.append(
            [
                "COLUMN_MISMATCH",
                f"Invalid column(s) found in source data: {extras_in_csv}.",
            ]
        )

    not_found_in_csv = metadata_column_names.difference(csv_headers)
    if not_found_in_csv:
        errors.append(
            [
                "COLUMN_MISMATCH",
                f"Column(s) missing from source data: {not_found_in_csv}.",
            ]
        )

    # Validate Data in Columns
    for col in metadata["columns"]:
        col_name = col["name"]
        if col_name in not_found_in_csv:
            continue

        match col["data_type"]:
            case "bbl":
                with_invalid_bbl = csv_df[~csv_df["BBL"].str.match("^\d{10}$")]
                if not with_invalid_bbl.empty:
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"Column {col_name} contains {len(with_invalid_bbl)} invalid records, for example: {with_invalid_bbl.iloc[0][col_name]}",
                        ]
                    )

            case "integer":

                def is_maybe_int(s):
                    if len(s) == 0 or not s:
                        return True
                    if s[0] in ("-", "+"):
                        return s[1:].isdigit()
                    return s.isdigit()

                invalids = csv_df_non_null[
                    ~csv_df_non_null[col_name].apply(is_maybe_int)
                ]
                if not invalids.empty:
                    sample = invalids.iloc[0][col_name]
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"The {col_name} column contains {len(invalids)} invalid integer types, for example: {sample}",
                        ]
                    )

            # TODO: discuss whether it makes sense to call these doubles. It's
            # just what they're designated as in COLP metadata.
            case "double":

                def is_maybe_float_or_double(s):
                    try:
                        float(s)
                        return True
                    except ValueError:
                        return False

                invalids = csv_df_non_null[
                    ~csv_df_non_null[col_name].apply(is_maybe_float_or_double)
                ]
                if not invalids.empty:
                    sample = invalids.iloc[0][col_name]
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"The {col_name} column contains {len(invalids)} invalid double types, for example: {sample}",
                        ]
                    )

            case "geometry":
                pass
            case "text":
                pass
            case _:
                errors.append(
                    [
                        "INVALID_METADATA",
                        f"Column {col_name} has unknown type {col['data_type']}.",
                    ]
                )

        # Validate standardized / enum values
        if "values" in col:
            accepted_values = {str(x[0]) for x in col["values"]} | {
                ""
            }  # Adding empty str so we can do null-checking elsewhere (in just one place)
            invalids = csv_df_non_null[~csv_df_non_null[col_name].isin(accepted_values)]
            if not invalids.empty:
                invalid_counts = dict(invalids.groupby(by=col_name).size())
                errors.append(
                    [
                        "INVALID_DATA",
                        f"Found counts of non-standardized values for column {col_name}: {invalid_counts}",
                    ]
                )

        if "is_nullable" in col and not col["is_nullable"]:
            nulls = csv_df[csv_df[col_name].isnull()]
            if not nulls.empty:
                errors.append(
                    [
                        "INVALID_DATA",
                        f"Column {col_name} has {len(nulls)} empty values",
                    ]
                )

    return errors
