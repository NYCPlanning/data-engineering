from functools import partial
import geopandas as gpd
import pandas as pd
from pathlib import Path
import shutil
from typing import Callable, Literal

from dcpy.models import file
from dcpy.models.lifecycle.ingest import FunctionCall
from dcpy.utils import data, introspect
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes

OUTPUT_GEOM_COLUMN = "geom"


def to_parquet(
    file_format: file.Format,
    local_data_path: Path,
    dir: Path,
    output_filename: str = "init.parquet",
) -> None:
    """
    Transforms raw data into a parquet file format and saves it locally.

    This function first checks for the presence of raw data locally at the specified `local_data_path`.
    The raw data is then read into a GeoDataFrame and saved as a parquet file.

    The transformation process varies depending on the format of the raw data, which can be in .shp, .gdb,
    .csv or zipped format. For csv files, if geometry is present, it is converted into a GeoSeries before creating
    the GeoDataFrame.

    Parameters:
        file_format_config (file.Format): Config object containing geometry info.
        local_data_path (Path): Path to the local data file.
        dir (Path): Directory to use for output file.
        output_filename (str): Output file name.

    Raises:
        AssertionError: `local_data_path` does not point to a valid file or directory.
        AssertionError: If `geom_column` is present in yaml template but not in the dataset.
    """

    # create new dir for output parquet file if doesn't exist
    dir.mkdir(parents=True, exist_ok=True)
    output_file_path = dir / output_filename

    assert (
        local_data_path.is_file() or local_data_path.is_dir()
    ), "Local path should be a valid file or directory"
    logger.info(f"✅ Raw data was found locally at {local_data_path}")

    gdf = data.read_data_to_df(file_format, local_data_path)

    # rename geom column to "geom" regardless of input data type
    if isinstance(gdf, gpd.GeoDataFrame):
        gdf = gdf.rename_geometry(OUTPUT_GEOM_COLUMN)

    gdf.to_parquet(output_file_path, index=False)
    logger.info(
        f"✅ Converted raw data to parquet file and saved as {output_file_path}"
    )


class Preprocessor:
    """
    This class is very much a first pass at something that would support the validate/run_processing_steps functions
    This should/will be iterated on when implementing actual preprocessing steps for chosen templates
    """

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name

    def sort(self, df: pd.DataFrame, by: list[str], ascending=False) -> pd.DataFrame:
        return df.sort_values(by=by, ascending=ascending)

    def filter_rows(
        self,
        df: pd.DataFrame,
        type: Literal["equals", "contains"],
        column_name: str | int,
        val: str | int,
    ) -> pd.DataFrame:
        if type == "equals":
            filter = df[column_name] == val
        elif type == "contains":
            filter = df[column_name].str.contains(str(val))
        return df[filter]

    def rename_columns(
        self, df: pd.DataFrame, map: dict[str, str], drop_others=False
    ) -> pd.DataFrame:
        df = df.rename(columns=map, errors="raise")
        if drop_others:
            df = df[list(map.values())]
        return df

    def clean_column_names(
        self,
        df: pd.DataFrame,
        *,
        replace: dict[str, str] | None = None,
        lower: bool = False,
    ) -> pd.DataFrame:
        replace = replace or {}
        columns = list(df.columns)
        for pattern in replace:
            columns = [c.replace(pattern, replace[pattern]) for c in columns]
        if lower:
            columns = [c.lower() for c in columns]
        df.columns = pd.Index(columns)
        return df

    def update_column(
        self,
        df: pd.DataFrame,
        column_name: str,
        val: str | int,
    ) -> pd.DataFrame:
        df[column_name] = val
        return df

    def append_prev(self, df: pd.DataFrame) -> pd.DataFrame:
        prev_df = recipes.read_df(
            recipes.Dataset(name=self.dataset_name, version="latest")
        )
        df = pd.concat((prev_df, df))
        return df

    def deduplicate(
        self,
        df: pd.DataFrame,
        sort_columns: list[str] | None = None,
        sort_ascending: bool = False,
        by: list[str] | None = None,
    ) -> pd.DataFrame:
        if sort_columns:
            df = df.sort_values(by=sort_columns, ascending=sort_ascending)
        by = by or []
        return df.drop_duplicates(by)

    def drop_columns(self, df: pd.DataFrame, columns: list[str | int]) -> pd.DataFrame:
        columns = [df.columns[i] if isinstance(i, int) else i for i in columns]
        return df.drop(columns, axis=1)

    def strip_columns(
        self, df: pd.DataFrame, cols: list[str] | None = None
    ) -> pd.DataFrame:
        if cols:
            for col in cols:
                df[col] = df[col].str.strip()
        else:
            df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        return df

    def pd_series_func(
        self,
        df: pd.DataFrame,
        column_name: str,
        function_name: str,
        output_column_name: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Operates on a given column using a given pandas Series function and supplied kwargs

        Example yml which defines a call:
        - name: pd_series_func
          args:
            column_name: jobnum
            function_name: str.replace
            pat: -[a-zA-Z\d]1$
            repl: ""
            regex: True

        Which has the effect of
        `df["jobnum"] = df["jobnum"].str.replace("-[a-zA-Z\d]1$", "", regex=True)`

        "function_name" must be a valid function of a pd.Series. This is validated
        kwargs are validated by name only, as annotations for these functions are quite messy

        function is called on "column_name" of df
        output of function is assigned to "column_name" (overwriting) unless 'output_column_name' if provided
        """
        parts = function_name.split(".")
        func = df[column_name]
        for part in parts:
            func = func.__getattribute__(part)
        df[output_column_name or column_name] = func(**kwargs)  # type: ignore
        return df

    def no_arg_function(self, df: pd.DataFrame) -> pd.DataFrame:
        """Dummy/stub for testing. Can be dropped if we implement actual function with no args other than df"""
        return df


def validate_pd_series_func(
    *, function_name: str, column_name: str = "", **kwargs
) -> str | dict[str, str]:
    parts = function_name.split(".")
    func = pd.Series()
    func_str = "pd.Series"
    for part in parts:
        if part not in func.__dir__():
            return f"'{func_str}' has no attribute '{part}'"
        func = func.__getattribute__(part)
        func_str += f".{part}"
    return introspect.validate_function_args(func, kwargs)  # type: ignore


def validate_processing_steps(
    dataset_name: str, processing_steps: list[FunctionCall]
) -> list[Callable]:
    """
    Given config of ingest dataset, violates that defined preprocessing steps
    exist and that appropriate arguments are supplied. Raises error detailing
    violations if any are found

    Returns list of callables, which expect a dataframe and return a dataframe
    """
    violations: dict[str, str | dict[str, str]] = {}
    compiled_steps: list[Callable] = []
    preprocessor = Preprocessor(dataset_name)
    for step in processing_steps:
        if step.name not in preprocessor.__dir__():
            violations[step.name] = "Function not found"
        else:
            func = getattr(preprocessor, step.name)

            # assume that function takes args "self, df"
            kw_error = introspect.validate_function_args(
                func, step.args, raise_error=False, ignore_args=["self", "df"]
            )
            if kw_error:
                violations[step.name] = kw_error

            # extra validation needed
            elif step.name == "pd_series_func":
                series_error = validate_pd_series_func(**step.args)
                if series_error:
                    violations[step.name] = series_error

            compiled_steps.append(partial(func, **step.args))

    if violations:
        raise Exception(f"Invalid preprocessing steps:\n{violations}")

    return compiled_steps


def run_processing_steps(
    dataset_name: str,
    processing_steps: list[FunctionCall],
    input_path: Path,
    output_path,
):
    """Validates and runs preprocessing steps defined in config object"""
    compiled_steps = validate_processing_steps(dataset_name, processing_steps)
    if len(compiled_steps) == 0:
        shutil.copy(input_path, output_path)
    else:
        df = pd.read_parquet(input_path)
        for step in compiled_steps:
            df = step(df)
        df.to_parquet(output_path)
