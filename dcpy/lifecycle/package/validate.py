from dataclasses import dataclass, field
from enum import Enum
import geopandas as gpd
import pandas as pd
from pathlib import Path
import pprint
from shapely import wkb, wkt
import typer
from pandera import Column, DataFrameSchema, Check, Index

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
    "bbl": lambda s: s.str.match(r"^\d{10}$"),
    "integer": lambda s: s.apply(_is_int),
    "double": lambda s: s.apply(_is_float_or_double),
    "wkb": lambda s: s.apply(_is_valid_wkb),
    "geom_point": lambda s: s.apply(_is_geom_point),
    "geom_poly": lambda s: s.apply(_is_geom_poly),
}


def validate_df(
    df: pd.DataFrame, dataset: models.DatasetFile, metadata: models.Metadata
) -> DatasetFileValidation:
    """Validate a dataframe against a metadata file."""

    def make_checks(col):
        vals_check = Check.isin([v[0] for v in col.values]) if col.values else None
        dcp_validator = col_validators.get(col.data_type)
        dcp_check = Check(dcp_validator) if dcp_validator else None

        return [c for c in [vals_check, dcp_check] if c is not None]

    shapefile_cols = dataset.get_columns(metadata)

    schema = DataFrameSchema(
        {c.name: Column(str, make_checks(c)) for c in shapefile_cols},
        index=Index(int),
        strict=True,
        coerce=True,
    )

    errors = []
    try:
        schema.validate(df)
    except Exception as e:
        errors = e

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
    package_path: Path, metadata: models.Metadata | None = None
) -> PackageValidation:
    metadata = metadata or models.Metadata.from_yaml(package_path / "metadata.yml")
    dataset_files_path = package_path / "dataset_files"
    validations = []

    for ds in metadata.package.dataset_files:
        ds_path = dataset_files_path / ds.filename
        try:
            match ds.type:
                case "csv":
                    logger.info(f"validating csv: {ds_path} for {ds.name}")
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
        if not (package_path / "attachments" / attachment).exists():
            package_errors.append(
                ValidationError(
                    error_type=ErrorType.MISSING_FILE,
                    message=f"Missing attachment: {attachment}",
                    dataset_file=None,
                )
            )

    unique_row_counts = {v.stats.row_count for v in validations}
    if len(unique_row_counts) > 1:
        package_errors.append(
            ValidationError(
                error_type=ErrorType.INVALID_DATA,
                message=f"Found varying row counts: {unique_row_counts}",
                dataset_file=None,
            )
        )
    return PackageValidation(validations=validations, errors=package_errors)


app = typer.Typer(add_completion=False)


@app.command("validate")
def _validate(
    package_path: Path,
    metadata_path: Path = typer.Option(
        None, "-m", "--metadata-path", help="(Optional) Metadata Path"
    ),
):
    validation = validate_package(
        package_path,
        models.Metadata.from_yaml(metadata_path or package_path / "metadata.yml"),
    )
    validation.pretty_print_errors()
