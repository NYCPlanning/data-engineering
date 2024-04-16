import duckdb
from functools import partial
import geopandas as gpd
import inspect
import pandas as pd
import shutil
from typing import Any, Callable

from dcpy.utils import s3

from pathlib import Path

from dcpy.models import file
from dcpy.models.lifecycle.ingest import Config, FunctionCall
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes
from . import TMP_DIR, PARQUET_PATH, configure


def to_parquet(config: Config, local_data_path: Path | None = None):
    """
    Transforms raw data into a parquet file format and saves it locally.

    This function first checks for the presence of raw data locally at the specified `local_data_path`.
    If the path not provided, it is downloaded from an S3 bucket using the `config` parameter.
    The raw data is then read into a GeoDataFrame and saved as a parquet file.

    The transformation process varies depending on the format of the raw data, which can be in .shp, .gdb,
    or .csv format. For csv files, if geometry is present, it is converted into a GeoSeries before creating
    the GeoDataFrame.

    Parameters:
        config (recipes.ExtractConfig): Config object containing geometry info.
        local_data_path (Path, optional): Path to the local data file. If not provided, data is pulled from S3 bucket.

    Raises:
        AssertionError: If `local_data_path` is provided but does not point to a valid file or directory.
        AssertionError: If `geom_column` is present in yaml template but not in the dataset.
    """

    # create new dir for raw data and output parquet file
    if TMP_DIR.is_dir():
        shutil.rmtree(TMP_DIR)
    TMP_DIR.mkdir()

    if local_data_path:
        assert (
            local_data_path.is_file() or local_data_path.is_dir()
        ), "Local path should be a valid file or directory"
        logger.info(f"✅ Raw data was found locally at {local_data_path}")
    else:
        local_data_path = TMP_DIR / config.raw_filename

        s3.download_file(
            bucket=recipes.BUCKET,
            key=str(config.raw_dataset_s3_filepath),
            path=local_data_path,
        )
        logger.info(f"Downloaded raw data from s3 to {local_data_path}")

    data_load_config = config.file_format

    # TODO: rename geom column to "geom" regardless of input data type
    match data_load_config:
        case file.Shapefile() as shapefile:
            gdf = gpd.read_file(
                local_data_path,
                crs=shapefile.crs,
                encoding=shapefile.encoding,
            )
        case file.Geodatabase() as geodatabase:
            gdf = gpd.read_file(
                local_data_path,
                crs=geodatabase.crs,
                encoding=geodatabase.encoding,
                layer=geodatabase.layer,
            )
        case file.Csv() as csv:
            df = pd.read_csv(
                local_data_path,
                index_col=False,
                encoding=data_load_config.encoding,
                delimiter=data_load_config.delimiter,
            )

            if not csv.geometry:
                gdf = df

            else:
                # case when geometry is in one column (i.e. polygon or point object type)
                if isinstance(csv.geometry.geom_column, str):
                    geom_column = csv.geometry.geom_column
                    assert (
                        geom_column in df.columns
                    ), f"❌ Geometry column specified in recipe template does not exist in {config.raw_filename}"

                    # replace NaN values with None. Otherwise gpd throws an error
                    if df[geom_column].isnull().any():
                        df[geom_column] = df[geom_column].astype(object)
                        df[geom_column] = df[geom_column].where(
                            df[geom_column].notnull(), None
                        )

                    df[geom_column] = gpd.GeoSeries.from_wkt(df[geom_column])

                    gdf = gpd.GeoDataFrame(
                        df,
                        geometry=geom_column,
                        crs=csv.geometry.crs,
                    )

    gdf.to_parquet(PARQUET_PATH, index=False)
    logger.info(f"✅ Converted raw data to parquet file and saved as {PARQUET_PATH}")


class Preprocessors:
    """
    This class is very much a first pass at something that would support the validate/run_processing_steps functions
    This should/will be iterated on when implementing actual preprocessing steps for chosen templates
    """

    @staticmethod
    def split_column(
        df: pd.DataFrame,
        col: str,
        target_cols: list[str],
        splitter: Callable[[Any], list[Any]],
        keep_col=False,
    ) -> pd.DataFrame:
        df[target_cols] = df[col].apply(splitter)
        if not keep_col:
            df.drop(col, axis=1, inplace=True)
        return df

    @staticmethod
    def drop_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        columns = [df.columns[i] if isinstance(i, int) else i for i in columns]
        return df.drop(columns, axis=1)

    @staticmethod
    def strip_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        if cols == []:
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        else:
            for col in cols:
                df[col] = df[col].str.strip()
        return df

    @staticmethod
    def no_arg_function(df: pd.DataFrame) -> pd.DataFrame:
        """Dummy/stub for testing. Can be dropped if we implement actual function with no args other than df"""
        return df

    @staticmethod
    def append_prev(df: pd.DataFrame, dataset: str, version: str) -> pd.DataFrame:
        prev_df = recipes.read_df(recipes.Dataset(name=dataset, version=version))
        df = pd.concat((prev_df, df))
        return df

    @staticmethod
    def append_prev_duckdb(file: Path, dataset: str, version: str):
        duckdb.sql(
            f"""
            COPY (
                SELECT * FROM 's3://edm-recipes/datasets/{dataset}/{version}/{dataset}.parquet'
                UNION ALL
                SELECT * FROM '{file}'
            ) TO '{file}' (FORMAT PARQUET)"""
        )


def validate_processing_steps(steps: list[FunctionCall]) -> list[Callable]:
    """
    Given config of ingest dataset, violates that defined preprocessing steps
    exist and that appropriate arguments are supplied. Raises error detailing
    violations if any are found

    Returns list of callables, which expect a dataframe and return a dataframe
    """
    violations: dict[str, str | dict[str, str]] = {}
    compiled_steps: list[Callable] = []
    for step in steps:
        if step.name not in Preprocessors().__dir__():
            violations[step.name] = "Function not found"
        else:
            func = getattr(Preprocessors(), step.name)

            kwargs = step.args.copy()
            # assume that function takes arg "df"
            kwargs["df"] = pd.DataFrame()
            kw_error = configure.validate_function_args(func, kwargs, raise_error=False)
            if kw_error:
                violations[step.name] = kw_error

            compiled_steps.append(partial(func, **step.args))

    if violations:
        raise Exception(f"Invalid preprocessing steps:\n{violations}")

    return compiled_steps


def run_processing_steps(steps: list[FunctionCall], local_data_path: Path) -> Path:
    """Validates and runs preprocessing steps defined in config object"""
    compiled_steps = validate_processing_steps(steps)
    if len(steps) == 0:
        return local_data_path
    else:
        df = pd.read_parquet(local_data_path)
        for step in compiled_steps:
            df = step(df)
        df.to_parquet(local_data_path)
        return local_data_path
