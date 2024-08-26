import ast
from dataclasses import dataclass, field
from enum import Enum
import geopandas as gpd
import pandas as pd
from pathlib import Path
import pprint
from shapely import wkb, wkt
import typer

import dcpy.models.product.dataset.metadata_v2 as dataset_md
from dcpy.utils.logging import logger


class ErrorType(Enum):
    INVALID_DATA = "INVALID_DATA"
    NULLS_FOUND = "NULLS_FOUND"
    COLUMM_MISMATCH = "COLUMN_MISMATCH"
    INVALID_METADATA = "INVALID_METADATA"
    UNHANDLED_EXCEPTION = "UNHANDLED_EXCEPTION"
    MISSING_FILE = "MISSING_FILE"


@dataclass
class ValidationError:
    error_type: ErrorType
    message: str


@dataclass
class ValidationStats:
    row_count: int | None


@dataclass
class DatasetFileValidation:
    dataset_file_id: str
    stats: ValidationStats = field(
        default_factory=lambda: ValidationStats(row_count=None)
    )
    errors: list[ValidationError] = field(default_factory=lambda: [])


@dataclass
class PackageValidation:
    file_validations: list[DatasetFileValidation]
    errors: list[ValidationError]

    def pretty_print_errors(self):
        for file_validation in self.file_validations:
            for fe in file_validation.errors:
                pprint.pp(
                    [
                        file_validation.dataset_file_id,
                        fe.error_type.value,
                        fe.message,
                    ]
                )
        for error in self.errors:
            pprint.pp(
                [
                    "package",
                    error.error_type.value,
                    error.message,
                ]
            )


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


def _is_geom_point(s):
    try:
        return wkt.loads(s).geom_type == "Point"
    except ValueError:
        return False


def _is_geom_poly(s):
    try:
        return wkt.loads(s).geom_type in {"Polygon", "MultiPolygon"}
    except ValueError:
        return False


col_validators = {
    "bbl": lambda df, col_name: df[~df[col_name].str.match(r"^\d{10}$")],
    "integer": lambda df, col_name: df[~df[col_name].apply(_is_int)],
    "double": lambda df, col_name: df[~df[col_name].apply(_is_float_or_double)],
    "wkb": lambda df, col_name: df[~df[col_name].apply(_is_valid_wkb)],
    "geom_point": lambda df, col_name: df[~df[col_name].apply(_is_geom_point)],
    "geom_poly": lambda df, col_name: df[~df[col_name].apply(_is_geom_poly)],
    "decimal": lambda df, col_name: df[~df[col_name].str.match(r"^-?\d*\.\d*$")],
    # TODO
    "datetime": lambda df, col_name: df.iloc[0:0],
    "uid": lambda df, col_name: df.iloc[0:0],
    "boro_code": lambda df, col_name: df.iloc[0:0],
    "block": lambda df, col_name: df.iloc[0:0],
    "lot": lambda df, col_name: df.iloc[0:0],
    "latitude": lambda df, col_name: df.iloc[0:0],
    "longitude": lambda df, col_name: df.iloc[0:0],
    "text": lambda df, col_name: df.iloc[0:0],
}


def validate_df(
    df: pd.DataFrame, columns: list[dataset_md.DatasetColumn]
) -> list[ValidationError]:
    """Validate a dataframe against a metadata file."""
    df_stringified_nulls = df.fillna("")

    errors = []

    column_names = {c.name for c in columns}

    # Find mismatched columns
    df_headers = set(df.columns)

    extras_in_source = sorted(list(df_headers.difference(column_names)))
    if extras_in_source:
        errors.append(
            ValidationError(
                error_type=ErrorType.COLUMM_MISMATCH,
                message=f"Invalid column(s) found in source data: {extras_in_source}.",
            )
        )

    not_found_in_source = sorted(list(column_names.difference(df_headers)))
    if not_found_in_source:
        errors.append(
            ValidationError(
                error_type=ErrorType.COLUMM_MISMATCH,
                message=f"Column(s) missing from source data: {not_found_in_source}.",
            )
        )

    # Validate Data in Columns
    for col in columns:
        if col.name in not_found_in_source:
            continue

        col_type = type(df.dtypes.get(col.name))

        if col_type is gpd.array.GeometryDtype:
            continue

        df_no_col_nulls = df_stringified_nulls[
            df_stringified_nulls[col.name].apply(str).str.len() > 0
        ]
        df_only_col_nulls = df_stringified_nulls[
            df_stringified_nulls[col.name].str.len() == 0
        ]

        # Check for unknown types
        if col.data_type not in col_validators:
            errors.append(
                ValidationError(
                    error_type=ErrorType.INVALID_METADATA,
                    message=f"Column {col.name} has unknown type {col.data_type}.",
                )
            )
            continue

        # Validate that data in columns matches declared types, for non-nulls only
        invalids = col_validators[col.data_type](df_no_col_nulls, col.name)
        if not invalids.empty:
            errors.append(
                ValidationError(
                    error_type=ErrorType.INVALID_DATA,
                    message=f"Column {col.name} contains {len(invalids)} invalid record(s), for example: {invalids.iloc[0][col.name]}",
                )
            )

        # Validate standardized/enum values
        if col.values:
            accepted_values = {str(v[0]) for v in col.values} | {""}
            invalids = df_no_col_nulls[~df_no_col_nulls[col.name].isin(accepted_values)]
            if not invalids.empty:
                invalid_counts = dict(invalids.groupby(by=col.name).size())
                invalid_counts_int = {k: int(v) for k, v in invalid_counts.items()}
                errors.append(
                    ValidationError(
                        error_type=ErrorType.INVALID_DATA,
                        message=f"Found counts of non-standardized values for column {col.name}: {invalid_counts_int}",
                    )
                )

        # Check Nulls
        if col.checks and col.checks.non_nullable:
            if not df_only_col_nulls.empty:
                errors.append(
                    ValidationError(
                        error_type=ErrorType.NULLS_FOUND,
                        message=f"Column {col.name} has {len(df_only_col_nulls)} empty values",
                    )
                )

    return errors


def validate_csv(
    csv_path: Path, columns: list[dataset_md.DatasetColumn]
) -> list[ValidationError]:
    df = pd.read_csv(csv_path, dtype=str)
    return validate_df(df, columns)


def validate_shapefile(
    shp_path: Path, columns: list[dataset_md.DatasetColumn]
) -> list[ValidationError]:
    df = pd.DataFrame(gpd.read_file(shp_path), dtype=str)
    return validate_df(df, columns)


def validate_package_files(
    package_path: Path, metadata: dataset_md.Metadata
) -> list[DatasetFileValidation]:
    dataset_files_path = (
        package_path / "dataset_files"
    )  # TODO: wrong place for this calculation. Move it.
    file_validations: list[DatasetFileValidation] = []

    for md_file_with_overrides in metadata.files:
        md_file = md_file_with_overrides.file

        if not md_file.is_metadata:
            ds_path = dataset_files_path / md_file.filename
            try:
                dataset = metadata.calculate_file_dataset_metadata(file_id=md_file.id)

                match md_file.type:
                    case "csv":
                        logger.info(f"validating csv: {ds_path} for {md_file.id}")
                        file_validations.append(
                            DatasetFileValidation(
                                dataset_file_id=md_file.id,
                                errors=validate_csv(ds_path, dataset.columns),
                            )
                        )
                    case "shapefile":
                        file_validations.append(
                            DatasetFileValidation(
                                dataset_file_id=md_file.id,
                                errors=validate_shapefile(ds_path, dataset.columns),
                            )
                        )
                    case _:
                        pass
            except Exception as e:
                file_validations.append(
                    DatasetFileValidation(
                        dataset_file_id=md_file.id,
                        errors=[
                            ValidationError(
                                error_type=ErrorType.UNHANDLED_EXCEPTION,
                                message=str(e),
                            )
                        ],
                    )
                )
        else:
            if not (package_path / "attachments" / md_file.filename).exists():
                file_validations.append(
                    DatasetFileValidation(
                        dataset_file_id=md_file.id,
                        errors=[
                            ValidationError(
                                error_type=ErrorType.MISSING_FILE,
                                message=f"Missing attachment: {md_file.id}",
                            )
                        ],
                    )
                )

    return file_validations


def validate_package_from_path(
    package_path: Path,
    metadata_override_path: Path | None = None,
    metadata_args: dict | None = None,
) -> list[DatasetFileValidation]:
    logger.info(f"Validating package at {package_path}")
    metadata = dataset_md.Metadata.from_path(
        metadata_override_path or (package_path / "metadata.yml"),
        template_vars=metadata_args,
    )
    return validate_package_files(package_path=package_path, metadata=metadata)


app = typer.Typer()


@app.command()
def _validate(
    package_path: Path,
    metadata_path: Path = typer.Option(
        None, "-m", "--metadata-path", help="(Optional) Metadata Path"
    ),
    metadata_args: str = typer.Option(default={}, callback=ast.literal_eval),
):
    validations = validate_package_from_path(
        package_path,
        metadata_path,
        metadata_args=metadata_args,  # type: ignore
    )
    for f in validations:
        for e in f.errors:
            print(
                f"{f.dataset_file_id[0:20].ljust(20)} | {e.error_type.value[0:20].ljust(20)} | {e.message}"
            )
