import pandas as pd
from pathlib import Path
from shapely import wkb
import typer

from . import models
from dcpy.utils.logging import logger


def validate_df(df: pd.DataFrame, dataset: models.Dataset, metadata: models.Metadata):
    """Validate a dataframe against a metadata file."""
    df_non_null = df.fillna("")

    errors = []

    dataset_columns = dataset.get_columns(metadata)
    dataset_column_names = {c.name for c in dataset_columns}

    # Find mismatched columns
    df_headers = set(df.columns)

    extras_in_source = df_headers.difference(dataset_column_names)
    if extras_in_source:
        errors.append(
            [
                "COLUMN_MISMATCH",
                f"Invalid column(s) found in source data: {extras_in_source}.",
            ]
        )

    not_found_in_source = dataset_column_names.difference(df_headers)
    if not_found_in_source:
        errors.append(
            [
                "COLUMN_MISMATCH",
                f"Column(s) missing from source data: {not_found_in_source}.",
            ]
        )

    # Validate Data in Columns
    for col in dataset_columns:
        if col.name in not_found_in_source:
            continue

        match col.data_type:
            case "bbl":
                with_invalid_bbl = df[~df["bbl"].str.match(r"^\d{10}$")]
                if not with_invalid_bbl.empty:
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"Column {col.name} contains {len(with_invalid_bbl)} invalid records, for example: {with_invalid_bbl.iloc[0][col.name]}",
                        ]
                    )

            case "integer":

                def is_maybe_int(s):
                    if len(s) == 0 or not s:
                        return True
                    if s[0] in ("-", "+"):
                        return s[1:].isdigit()
                    return s.isdigit()

                invalids = df_non_null[~df_non_null[col.name].apply(is_maybe_int)]
                if not invalids.empty:
                    sample = invalids.iloc[0][col.name]
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"The {col.name} column contains {len(invalids)} invalid integer types, for example: {sample}",
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

                invalids = df_non_null[
                    ~df_non_null[col.name].apply(is_maybe_float_or_double)
                ]
                if not invalids.empty:
                    sample = invalids.iloc[0][col.name]
                    errors.append(
                        [
                            "INVALID_DATA",
                            f"The {col.name} column contains {len(invalids)} invalid double types, for example: {sample}",
                        ]
                    )

            case "wkb":

                def is_wkb_valid(g):
                    try:
                        wkb.loads(g)
                        return True
                    except Exception:
                        return False

                invalids = df[~df[col.name].apply(is_wkb_valid)]
                if not invalids.empty:
                    sample = invalids.iloc[0][col.name]

                    sample_error = None
                    try:
                        wkb.loads(sample)
                    except Exception as e:
                        sample_error = e

                    errors.append(
                        [
                            "INVALID_DATA",
                            f"The {col.name} column contains {len(invalids)} invalid wkb value(s), for example: {sample}. \
                            error: {sample_error}",
                        ]
                    )

            case "uid":
                pass
            case "boro_code":
                pass
            case "block":
                pass
            case "lot":
                pass
            case "latitude":
                pass
            case "longitude":
                pass

            case "text":
                pass
            case _:
                errors.append(
                    [
                        "INVALID_METADATA",
                        f"Column {col.name} has unknown type {col.data_type}.",
                    ]
                )

        # Validate standardized / enum values
        if col.values:
            accepted_values = {str(v[0]) for v in col.values} | {
                ""
            }  # Adding empty str so we can do null-checking elsewhere (in just one place)
            invalids = df_non_null[~df_non_null[col.name].isin(accepted_values)]
            if not invalids.empty:
                invalid_counts = dict(invalids.groupby(by=col.name).size())
                errors.append(
                    [
                        "INVALID_DATA",
                        f"Found counts of non-standardized values for column {col.name}: {invalid_counts}",
                    ]
                )

        if col.is_nullable:
            nulls = df[df[col.name].isnull()]

            if not nulls.empty:
                errors.append(
                    [
                        "INVALID_DATA",
                        f"Column {col.name} has {len(nulls)} empty values",
                    ]
                )

    return errors


def validate_csv(csv_path: Path, dataset: models.Dataset, metadata: models.Metadata):
    df = pd.read_csv(csv_path, dtype=str)
    return validate_df(df, dataset, metadata)


def validate_package(package_path: Path, metadata: models.Metadata):
    for ds in metadata.dataset_package.datasets:
        match ds.type:
            case "csv":
                csv_path = package_path / ds.filename
                logger.info(f"validating csv: {csv_path} for {ds.name}")
                return validate_csv(package_path / ds.filename, ds, metadata)


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


if __name__ == "__main__":
    app()
