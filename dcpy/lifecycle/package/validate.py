import pandas as pd
from pathlib import Path
from shapely import wkb
import typer

import dcpy.models.product_metadata as models
from dcpy.utils.logging import logger


class Errors:
    INVALID_DATA = "INVALID_DATA"
    NULLS_FOUND = "NULLS_FOUND"
    COLUMM_MISMATCH = "COLUMN_MISMATCH"
    INVALID_METADATA = "INVALID_METADATA"


def _is_valid_wkb(g):
    if not g:
        return True
    try:
        wkb.loads(g)
        return True
    except Exception:
        return False


def _is_float_or_double(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def _is_int(s):
    if s[0] in ("-", "+"):
        return s[1:].isdigit()
    return s.isdigit()


col_validators = {
    "bbl": lambda df, col_name: df[~df[col_name].str.match(r"^\d{10}$")],
    "integer": lambda df, col_name: df[~df[col_name].apply(_is_int)],
    "double": lambda df, col_name: df[~df[col_name].apply(_is_float_or_double)],
    "wkb": lambda df, col_name: df[~df[col_name].apply(_is_valid_wkb)],
    # TODO
    "uid": lambda df, col_name: df.iloc[0:0],
    "boro_code": lambda df, col_name: df.iloc[0:0],
    "block": lambda df, col_name: df.iloc[0:0],
    "lot": lambda df, col_name: df.iloc[0:0],
    "latitude": lambda df, col_name: df.iloc[0:0],
    "longitude": lambda df, col_name: df.iloc[0:0],
    "text": lambda df, col_name: df.iloc[0:0],
}


def validate_df(
    df: pd.DataFrame, dataset: models.DatasetFile, metadata: models.Metadata
) -> list[tuple[str, str]]:
    """Validate a dataframe against a metadata file."""
    df_stringified_nulls = df.fillna("")

    errors = []

    dataset_columns = dataset.get_columns(metadata)
    dataset_column_names = {c.name for c in dataset_columns}

    # Find mismatched columns
    df_headers = set(df.columns)

    extras_in_source = df_headers.difference(dataset_column_names)
    if extras_in_source:
        errors.append(
            (
                Errors.COLUMM_MISMATCH,
                f"Invalid column(s) found in source data: {extras_in_source}.",
            )
        )

    not_found_in_source = dataset_column_names.difference(df_headers)
    if not_found_in_source:
        errors.append(
            (
                Errors.COLUMM_MISMATCH,
                f"Column(s) missing from source data: {not_found_in_source}.",
            )
        )

    # Validate Data in Columns
    for col in dataset_columns:
        if col.name in not_found_in_source:
            continue

        df_no_col_nulls = df_stringified_nulls[
            df_stringified_nulls[col.name].str.len() > 0
        ]
        df_only_col_nulls = df_stringified_nulls[
            df_stringified_nulls[col.name].str.len() == 0
        ]

        # Check for unknown types
        if col.data_type not in col_validators:
            errors.append(
                (
                    Errors.INVALID_METADATA,
                    f"Column {col.name} has unknown type {col.data_type}.",
                )
            )
            continue

        # Validate that data in columns matches declared types, for non-nulls only
        invalids = col_validators[col.data_type](df_no_col_nulls, col.name)
        if not invalids.empty:
            errors.append(
                (
                    Errors.INVALID_DATA,
                    f"Column {col.name} contains {len(invalids)} invalid record(s), for example: {invalids.iloc[0][col.name]}",
                )
            )

        # Validate standardized/enum values
        if col.values:
            accepted_values = {str(v[0]) for v in col.values} | {""}
            invalids = df_no_col_nulls[~df_no_col_nulls[col.name].isin(accepted_values)]
            if not invalids.empty:
                invalid_counts = dict(invalids.groupby(by=col.name).size())
                errors.append(
                    (
                        Errors.INVALID_DATA,
                        f"Found counts of non-standardized values for column {col.name}: {invalid_counts}",
                    )
                )

        # Check Nulls
        if col.non_nullable:
            if not df_only_col_nulls.empty:
                errors.append(
                    (
                        Errors.NULLS_FOUND,
                        f"Column {col.name} has {len(df_only_col_nulls)} empty values",
                    )
                )

    return errors


def validate_csv(
    csv_path: Path, dataset: models.DatasetFile, metadata: models.Metadata
):
    df = pd.read_csv(csv_path, dtype=str)
    return validate_df(df, dataset, metadata)


def validate_package(package_path: Path, metadata: models.Metadata):
    errors = []
    for ds in metadata.package.dataset_files:
        match ds.type:
            case "csv":
                csv_path = package_path / ds.filename
                logger.info(f"validating csv: {csv_path} for {ds.name}")
                errors += validate_csv(package_path / ds.filename, ds, metadata)
            case _:
                pass
    return errors


app = typer.Typer(add_completion=False)


@app.command("validate")
def _validate(
    package_path: Path = typer.Option(
        None,
        "-p",
        "--package-path",
        help="Package Path",
    ),
    metadata_path: Path = typer.Option(
        None, "-m", "--metadata-path", help="(Optional) Metadata Path"
    ),
):
    errors = validate_package(
        package_path,
        models.Metadata.from_yaml(metadata_path or package_path / "metadata.yml"),
    )
    logger.info(f"errors: {errors}")
