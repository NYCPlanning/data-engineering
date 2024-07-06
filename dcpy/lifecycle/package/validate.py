import ast
from dataclasses import dataclass, field
from enum import Enum
import geopandas as gpd
import pandas as pd
from pathlib import Path
import pprint
from shapely import wkb, wkt
import typer

import dcpy.models.product.dataset.metadata as models
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
    dataset_file: models.DatasetFile | None


@dataclass
class ValidationStats:
    row_count: int | None


@dataclass
class DatasetFileValidation:
    stats: ValidationStats = field(
        default_factory=lambda: ValidationStats(row_count=None)
    )
    errors: list[ValidationError] = field(default_factory=lambda: [])


@dataclass
class PackageValidation:
    validations: list[DatasetFileValidation]
    errors: list[ValidationError]

    def get_dataset_errors(self) -> list[ValidationError]:
        """Get validation errors from all dataset validations in the package."""
        return sum([v.errors for v in self.validations if v.errors], [])

    def pretty_print_errors(self):
        ds_errors = self.get_dataset_errors()

        for e in ds_errors + self.errors:
            pprint.pp(
                [
                    e.dataset_file.filename if e.dataset_file else None,
                    e.error_type.value,
                    e.message,
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
    df: pd.DataFrame, dataset: models.DatasetFile, metadata: models.Metadata
) -> DatasetFileValidation:
    """Validate a dataframe against a metadata file."""
    df_stringified_nulls = df.fillna("")

    errors = []

    ignored_cols = set(dataset.overrides.ignore_validation)
    dataset_columns = [
        col for col in dataset.get_columns(metadata) if col.name not in ignored_cols
    ]
    dataset_column_names = {c.name for c in dataset_columns}

    # Find mismatched columns
    df_headers = set(df.columns)

    extras_in_source = sorted(
        list(df_headers.difference(dataset_column_names) - ignored_cols)
    )
    if extras_in_source:
        errors.append(
            ValidationError(
                error_type=ErrorType.COLUMM_MISMATCH,
                message=f"Invalid column(s) found in source data: {extras_in_source}.",
                dataset_file=dataset,
            )
        )

    not_found_in_source = sorted(
        list(dataset_column_names.difference(df_headers) - ignored_cols)
    )
    if not_found_in_source:
        errors.append(
            ValidationError(
                error_type=ErrorType.COLUMM_MISMATCH,
                message=f"Column(s) missing from source data: {not_found_in_source}.",
                dataset_file=dataset,
            )
        )

    # Validate Data in Columns
    for col in dataset_columns:
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
                    dataset_file=dataset,
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
                    dataset_file=dataset,
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
                        dataset_file=dataset,
                    )
                )

        # Check Nulls
        if col.non_nullable:
            if not df_only_col_nulls.empty:
                errors.append(
                    ValidationError(
                        error_type=ErrorType.NULLS_FOUND,
                        message=f"Column {col.name} has {len(df_only_col_nulls)} empty values",
                        dataset_file=dataset,
                    )
                )

    return DatasetFileValidation(
        errors=errors, stats=ValidationStats(row_count=len(df))
    )


def validate_csv(
    csv_path: Path, dataset: models.DatasetFile, metadata: models.Metadata
) -> DatasetFileValidation:
    df = pd.read_csv(csv_path, dtype=str)
    return validate_df(df, dataset, metadata)


def validate_shapefile(
    shp_path: Path, dataset: models.DatasetFile, metadata: models.Metadata
) -> DatasetFileValidation:
    df = pd.DataFrame(gpd.read_file(shp_path), dtype=str)
    return validate_df(df, dataset, metadata)


def validate_package(
    package_path: Path, metadata: models.Metadata
) -> PackageValidation:
    dataset_files_path = package_path / "dataset_files"
    validations = []

    for ds in metadata.package.dataset_files:
        ds_path = dataset_files_path / ds.filename
        try:
            match ds.type:
                case "csv":
                    logger.info(f"validating csv: {ds_path} for {ds.id}")
                    validations.append(validate_csv(ds_path, ds, metadata))
                case "shapefile":
                    validations.append(validate_shapefile(ds_path, ds, metadata))
                case _:
                    pass
        except Exception as e:
            validations.append(
                DatasetFileValidation(
                    errors=[
                        ValidationError(
                            error_type=ErrorType.UNHANDLED_EXCEPTION,
                            message=str(e),
                            dataset_file=ds,
                        )
                    ]
                )
            )

    package_errors = []

    for attachment in metadata.package.attachments:
        if not (package_path / "attachments" / attachment.filename).exists():
            package_errors.append(
                ValidationError(
                    error_type=ErrorType.MISSING_FILE,
                    message=f"Missing attachment: {attachment}",
                    dataset_file=None,
                )
            )

    return PackageValidation(validations=validations, errors=package_errors)


def validate_package_from_path(
    package_path: Path,
    metadata_override_path: Path | None = None,
    metadata_args: dict | None = None,
) -> PackageValidation:
    metadata = models.Metadata.from_path(
        (metadata_override_path or package_path) / "metadata.yml",
        template_vars=metadata_args,
    )
    return validate_package(package_path=package_path, metadata=metadata)


app = typer.Typer()


@app.command()
def _validate(
    package_path: Path,
    metadata_path: Path = typer.Option(
        None, "-m", "--metadata-path", help="(Optional) Metadata Path"
    ),
    metadata_args: str = typer.Option(..., callback=ast.literal_eval),
):
    validation = validate_package_from_path(
        package_path,
        metadata_path,
        metadata_args=metadata_args,  # type: ignore
    )
    validation.pretty_print_errors()
