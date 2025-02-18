from functools import partial
import geopandas as gpd
import pandas as pd
from pathlib import Path
from typing import Callable, Literal

from dcpy.models import file
from dcpy.models.lifecycle.ingest import ProcessingStep, Column
from dcpy.utils import data, introspect
from dcpy.utils.geospatial import transform, parquet as geoparquet
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

    assert local_data_path.is_file() or local_data_path.is_dir(), (
        "Local path should be a valid file or directory"
    )
    logger.info(f"✅ Raw data was found locally at {local_data_path}")

    gdf = data.read_data_to_df(file_format, local_data_path)

    # rename geom column to "geom" regardless of input data type
    if isinstance(gdf, gpd.GeoDataFrame):
        gdf.rename_geometry(OUTPUT_GEOM_COLUMN, inplace=True)
        gdf[OUTPUT_GEOM_COLUMN] = gdf.make_valid()

    gdf.to_parquet(output_file_path, index=False)
    logger.info(
        f"✅ Converted raw data to parquet file and saved as {output_file_path}"
    )


class ProcessingFunctions:
    """
    This class is very much a first pass at something that would support the validate/run_processing_steps functions
    This should/will be iterated on when implementing actual processing steps for chosen templates
    """

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id

    def reproject(self, df: gpd.GeoDataFrame, target_crs: str) -> gpd.GeoDataFrame:
        return transform.reproject_gdf(df, target_crs=target_crs)

    def sort(self, df: pd.DataFrame, by: list[str], ascending=True) -> pd.DataFrame:
        sorted = df.sort_values(by=by, ascending=ascending)
        return sorted.reset_index(drop=True)

    def filter_rows(
        self,
        df: pd.DataFrame,
        type: Literal["equals", "contains"],
        column_name: str | int,
        val: str | int,
    ) -> pd.DataFrame:
        if type == "contains":
            filter = df[column_name].str.contains(str(val))
        else:
            filter = df[column_name] == val
        filtered = df[filter]
        return filtered.reset_index(drop=True)

    def filter_columns(
        self,
        df: pd.DataFrame,
        columns: list[str],
        mode: Literal["keep", "drop"] = "keep",
    ) -> pd.DataFrame:
        if mode == "keep":
            return df[columns]
        else:
            return df.drop(columns, axis=1)

    def rename_columns(
        self, df: pd.DataFrame, map: dict[str, str], drop_others=False
    ) -> pd.DataFrame:
        renamed = df.copy()
        if isinstance(renamed, gpd.GeoDataFrame) and renamed.geometry.name in map:
            renamed.rename_geometry(map.pop(renamed.geometry.name), inplace=True)
        renamed = renamed.rename(columns=map, errors="raise")
        if drop_others:
            renamed = renamed[list(map.values())]
        return renamed

    def clean_column_names(
        self,
        df: pd.DataFrame,
        *,
        replace: dict[str, str] | None = None,
        lower: bool = False,
        strip: bool = False,
    ) -> pd.DataFrame:
        cleaned = df.copy()
        replace = replace or {}
        columns = list(cleaned.columns)
        if strip:
            columns = [c.strip() for c in columns]
        for pattern in replace:
            columns = [c.replace(pattern, replace[pattern]) for c in columns]
        if lower:
            columns = [c.lower() for c in columns]
        cleaned.columns = pd.Index(columns)
        return cleaned

    def update_column(
        self,
        df: pd.DataFrame,
        column_name: str,
        val: str | int,
    ) -> pd.DataFrame:
        updated = df.copy()
        updated[column_name] = val
        return updated

    def append_prev(self, df: pd.DataFrame, version: str = "latest") -> pd.DataFrame:
        prev_df = recipes.read_df(recipes.Dataset(id=self.dataset_id, version=version))
        appended = pd.concat((prev_df, df))
        return appended.reset_index(drop=True)

    def upsert_column_of_previous_version(
        self,
        df: pd.DataFrame,
        key: list[str],
        version: str = "latest",
        insert_behavior: Literal["allow", "ignore", "error"] = "allow",
        missing_key_behavior: Literal["null", "coalesce", "error"] = "error",
    ) -> pd.DataFrame:
        assert key, "Must provide non-empty list of columns to be used as keys"
        prev_df = recipes.read_df(recipes.Dataset(id=self.dataset_id, version=version))
        df = data.upsert_df_columns(
            prev_df,
            df,
            key=key,
            insert_behavior=insert_behavior,
            missing_key_behavior=missing_key_behavior,
        )
        return df

    def deduplicate(
        self,
        df: pd.DataFrame,
        sort_columns: list[str] | None = None,
        sort_ascending: bool = True,
        by: list[str] | None = None,
    ) -> pd.DataFrame:
        deduped = df.copy()
        if sort_columns:
            deduped = deduped.sort_values(by=sort_columns, ascending=sort_ascending)
        deduped = deduped.drop_duplicates(by)
        return deduped.reset_index(drop=True)

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

    def coerce_column_types(
        self,
        df: pd.DataFrame,
        column_types: dict[
            str, Literal["numeric", "integer", "bigint", "string", "date", "datetime"]
        ],
        errors: Literal["raise", "coerce"] = "raise",
    ):
        def to_str(obj):
            if isinstance(obj, int):
                return str(obj)
            elif isinstance(obj, float) and obj.is_integer():
                return str(int(obj))
            elif pd.isna(obj):
                return None
            else:
                return str(obj)

        df = df.copy()
        for column in column_types:
            match column_types[column]:
                case "numeric":
                    df[column] = pd.to_numeric(df[column], errors=errors)
                case "integer" | "bigint" as t:
                    mapping = {"integer": "Int32", "bigint": "Int64"}
                    df[column] = pd.array(df[column], dtype=mapping[t])  # type: ignore
                case "string":
                    df[column] = df[column].apply(to_str)
                case "date":
                    df[column] = pd.to_datetime(df[column], errors=errors).dt.date
                    df[column] = df[column].replace(pd.NaT, None)  # type: ignore
                case "datetime":
                    df[column] = pd.to_datetime(df[column], errors=errors)
                    df[column] = df[column].replace(pd.NaT, None)  # type: ignore
        return df

    def multi(self, df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        multi_gdf = df.copy()
        multi_gdf.set_geometry(
            gpd.GeoSeries([transform.multi(feature) for feature in multi_gdf.geometry]),
            inplace=True,
        )
        return multi_gdf

    def pd_series_func(
        self,
        df: pd.DataFrame,
        column_name: str,
        function_name: str,
        output_column_name: str | None = None,
        geo: bool = False,  # only used for validation
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
        if geo and not isinstance(df, gpd.GeoDataFrame):
            raise TypeError(
                "GeoSeries processing function specified for non-geo df. Specify pd Series function instead, or ensure that gdf is read in properly"
            )
        transformed = df.copy()
        parts = function_name.split(".")
        func = transformed[column_name]
        for part in parts:
            func = func.__getattribute__(part)
        transformed[output_column_name or column_name] = func(**kwargs)  # type: ignore
        return transformed


def validate_pd_series_func(
    *, function_name: str, column_name: str = "", geo=False, **kwargs
) -> str | dict[str, str]:
    parts = function_name.split(".")
    if geo:
        func = gpd.GeoSeries()
        func_str = "gpd.GeoSeries"
    else:
        func = pd.Series()
        func_str = "pd.Series"
    for part in parts:
        if part not in func.__dir__():
            return f"'{func_str}' has no attribute '{part}'"
        func = func.__getattribute__(part)
        func_str += f".{part}"
    return introspect.validate_kwargs(func, kwargs)  # type: ignore


def validate_processing_steps(
    dataset_id: str, processing_steps: list[ProcessingStep]
) -> list[Callable]:
    """
    Given config of ingest dataset, violates that defined processing steps
    exist and that appropriate arguments are supplied. Raises error detailing
    violations if any are found

    Returns list of callables, which expect a dataframe and return a dataframe
    """
    violations: dict[str, str | dict[str, str]] = {}
    compiled_steps: list[Callable] = []
    processor = ProcessingFunctions(dataset_id)
    for step in processing_steps:
        if step.name not in processor.__dir__():
            violations[step.name] = "Function not found"
        else:
            func = getattr(processor, step.name)

            # assume that function takes args "self, df"
            kw_error = introspect.validate_kwargs(
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
        raise Exception(f"Invalid processing steps:\n{violations}")

    return compiled_steps


def validate_columns(df: pd.DataFrame, columns: list[Column]) -> None:
    """
    For now, simply validates that expected columns exists
    Does not validate data_type or other data checks
    """
    missing_columns = [c.id for c in columns if c.id not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Columns {missing_columns} defined in template but not found in processed dataset.\n Existing columns: {list(df.columns)}"
        )


def process(
    dataset_id: str,
    processing_steps: list[ProcessingStep],
    expected_columns: list[Column],
    input_path: Path,
    output_path: Path,
    output_csv: bool = False,
):
    """Validates and runs processing steps defined in config object"""
    df = geoparquet.read_df(input_path)
    compiled_steps = validate_processing_steps(dataset_id, processing_steps)

    for step in compiled_steps:
        df = step(df)

    validate_columns(df, expected_columns)

    if output_csv:
        df.to_csv(output_path.parent / f"{dataset_id}.csv")

    df.to_parquet(output_path)
