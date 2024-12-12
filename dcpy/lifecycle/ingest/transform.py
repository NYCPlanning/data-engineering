from functools import partial
import geopandas as gpd
import pandas as pd
from pathlib import Path
from typing import Callable, Literal

from dcpy.models import file
from dcpy.models.lifecycle.ingest import (
    ProcessingStep,
    Column,
    ProcessingSummary,
    ProcessingResult,
)
from dcpy.utils import data, introspect
from dcpy.utils.geospatial import transform, parquet as geoparquet
from dcpy.utils.logging import logger
from dcpy.connectors.edm import recipes

OUTPUT_GEOM_COLUMN = "geom"


def make_generic_change_stats(
    before: pd.DataFrame, after: pd.DataFrame, *, name: str, description: str
) -> ProcessingSummary:
    """Generate a ProcessingSummary by comparing two dataframes before and after processing."""
    initial_columns = set(before.columns)
    final_columns = set(after.columns)

    rows_added = len(after) - len(before)

    return ProcessingSummary(
        name=name,
        description=description,
        data_modifications={"rows_added": rows_added}
        if rows_added > 0
        else {"rows_removed": abs(rows_added)},
        column_modifications={
            "added": list(final_columns - initial_columns),
            "removed": list(initial_columns - final_columns),
        },
    )


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
    logger.info(f"Converting {local_data_path.name} to {output_filename}")

    gdf = data.read_data_to_df(file_format, local_data_path)

    # rename geom column to "geom" regardless of input data type
    if isinstance(gdf, gpd.GeoDataFrame):
        gdf.rename_geometry(OUTPUT_GEOM_COLUMN, inplace=True)
        gdf[OUTPUT_GEOM_COLUMN] = gdf.make_valid()

    gdf.to_parquet(output_file_path, index=False)


class ProcessingFunctions:
    """
    This class is very much a first pass at something that would support the validate/run_processing_steps functions
    This should/will be iterated on when implementing actual processing steps for chosen templates
    """

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self._REPROJECTION_DESCRIPTION_PREFIX = "Reprojected geometries"
        self._REPROJECTION_NOT_REQUIRED_DESCRIPTION = (
            "No reprojection required, as source and target crs are the same."
        )
        self._SORTED_BY_COLUMNS_DESCRIPTION_PREFIX = "Sorted by columns"

    def reproject(self, df: gpd.GeoDataFrame, target_crs: str) -> ProcessingResult:
        starting_crs = df.crs.to_string()
        needs_reproject = starting_crs != target_crs
        result = (
            transform.reproject_gdf(df, target_crs=target_crs)
            if needs_reproject
            else df
        )
        return ProcessingResult(
            df=result,
            summary=ProcessingSummary(
                name="reproject",
                # This should maybe just be a column_modification. We'd probably want the column name though.
                data_modifications={"rows_updated": len(df)} if needs_reproject else {},
                description=f"{self._REPROJECTION_DESCRIPTION_PREFIX} from {starting_crs} to {target_crs}"
                if needs_reproject
                else self._REPROJECTION_NOT_REQUIRED_DESCRIPTION,
            ),
        )

    def sort(self, df: pd.DataFrame, by: list[str], ascending=True) -> ProcessingResult:
        sorted = df.sort_values(by=by, ascending=ascending).reset_index(drop=True)
        summary = make_generic_change_stats(
            df,
            sorted,
            name="sort",
            description=f"{self._SORTED_BY_COLUMNS_DESCRIPTION_PREFIX}: {', '.join(by)}",
        )
        summary.data_modifications["rows_updated"] = (
            len(df) if not sorted.equals(df) else 0
        )
        return ProcessingResult(df=sorted, summary=summary)

    def filter_rows(
        self,
        df: pd.DataFrame,
        type: Literal["equals", "contains"],
        column_name: str | int,
        val: str | int,
    ) -> ProcessingResult:
        if type == "contains":
            filter = df[column_name].str.contains(str(val))
        else:
            filter = df[column_name] == val
        filtered = df[filter].reset_index(drop=True)
        return ProcessingResult(
            df=filtered,
            summary=ProcessingSummary(
                name="filter_rows",
                description="Filtered Rows",
                data_modifications={"rows_removed": len(df) - len(filtered)},
            ),
        )

    def filter_columns(
        self,
        df: pd.DataFrame,
        columns: list[str],
        mode: Literal["keep", "drop"] = "keep",
    ) -> ProcessingResult:
        filtered = df[columns] if mode == "keep" else df.drop(columns, axis=1)
        return ProcessingResult(
            df=filtered,
            summary=ProcessingSummary(
                name="filter_columns",
                description="filtered columns",
                column_modifications={
                    "dropped": set(df.columns) - set(filtered.columns)
                },
            ),
        )

    def rename_columns(
        self, df: pd.DataFrame, drop_others=False, **kwargs
    ) -> ProcessingResult:
        assert "map" in kwargs, "map must be supplied to rename_columns"
        col_map: dict[str, str] = kwargs[
            "map"
        ]  # doing this to avoid shadowing the builtin `map` fn
        renamed = df.copy()
        if isinstance(renamed, gpd.GeoDataFrame) and renamed.geometry.name in col_map:
            renamed.rename_geometry(col_map.pop(renamed.geometry.name), inplace=True)
        renamed = renamed.rename(columns=col_map, errors="raise")
        removed_cols = []
        if drop_others:
            renamed = renamed[list(col_map.values())]
            removed_cols = [
                col
                for col in (set(df.columns) - set(renamed.columns))
                if col not in col_map
            ]
        return ProcessingResult(
            df=renamed,
            summary=ProcessingSummary(
                name="rename_columns",
                description="Renamed columns",
                column_modifications={"renamed": col_map, "removed": removed_cols},
            ),
        )

    def clean_column_names(
        self,
        df: pd.DataFrame,
        *,
        replace: dict[str, str] | None = None,
        lower: bool = False,
        strip: bool = False,
    ) -> ProcessingResult:
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
        renamed_cols = {old: new for old, new in zip(df.columns, columns) if old != new}
        return ProcessingResult(
            df=cleaned,
            summary=ProcessingSummary(
                name="clean_column_names",
                description="Cleaned column names",
                column_modifications={"renamed": renamed_cols},
            ),
        )

    def update_column(
        self,
        df: pd.DataFrame,
        column_name: str,
        val: str | int,
    ) -> ProcessingResult:
        updated = df.copy()
        updated[column_name] = val
        return ProcessingResult(
            df=updated,
            summary=ProcessingSummary(
                name="update_column",
                description=f"Updated column '{column_name}' with value '{val}'",
                data_modifications={
                    "rows_updated": len(df)
                },  # assume we modified all rows
            ),
        )

    def append_prev(
        self, df: pd.DataFrame, version: str = "latest"
    ) -> ProcessingResult:
        prev_df = recipes.read_df(recipes.Dataset(id=self.dataset_id, version=version))
        appended = pd.concat((prev_df, df))
        appended = appended.reset_index(drop=True)
        summary = ProcessingSummary(
            name="append_prev",
            description=f"Appended rows from previous version: {version}",
            custom={"previous_version": version},
            data_modifications={"added": len(prev_df)},
        )
        return ProcessingResult(df=appended, summary=summary)

    def upsert_column_of_previous_version(
        self,
        df: pd.DataFrame,
        key: list[str],
        version: str = "latest",
        insert_behavior: Literal["allow", "ignore", "error"] = "allow",
        missing_key_behavior: Literal["null", "coalesce", "error"] = "error",
    ) -> ProcessingResult:
        assert key, "Must provide non-empty list of columns to be used as keys"
        prev_df = recipes.read_df(recipes.Dataset(id=self.dataset_id, version=version))
        df_initial_cols = set(df.columns)
        df = data.upsert_df_columns(
            prev_df,
            df,
            key=key,
            insert_behavior=insert_behavior,
            missing_key_behavior=missing_key_behavior,
        )
        summary = ProcessingSummary(
            name="upsert_column_of_previous_version",
            description="Appended rows",
            custom={
                "previous_version": version,
            },
            column_modifications={
                "added": sorted(list(set(prev_df.columns) - df_initial_cols))
            },
        )
        return ProcessingResult(df=df, summary=summary)

    def deduplicate(
        self,
        df: pd.DataFrame,
        sort_columns: list[str] | None = None,
        sort_ascending: bool = True,
        by: list[str] | None = None,
    ) -> ProcessingResult:
        deduped = df.copy()
        if sort_columns:
            deduped = deduped.sort_values(by=sort_columns, ascending=sort_ascending)
        deduped = deduped.drop_duplicates(by).reset_index(drop=True)
        summary = ProcessingSummary(
            name="deduplicate",
            description="Removed duplicates",
            data_modifications={"rows_removed": len(df) - len(deduped)},
        )
        return ProcessingResult(df=deduped, summary=summary)

    def drop_columns(
        self, df: pd.DataFrame, columns: list[str | int]
    ) -> ProcessingResult:
        columns = [df.columns[i] if isinstance(i, int) else i for i in columns]
        result = df.drop(columns, axis=1)
        summary = ProcessingSummary(
            name="drop_columns",
            description="Dropped columns",
            column_modifications={"dropped": columns},
        )
        return ProcessingResult(df=result, summary=summary)

    def strip_columns(
        self, df: pd.DataFrame, cols: list[str] | None = None
    ) -> ProcessingResult:
        stripped = df.copy()
        modifications = {}
        for col in cols or [c for c in df.columns if df[c].dtype == "object"]:
            stripped[col] = stripped[col].str.strip()
            modifications[col] = len(stripped[col].compare(df[col]))
        return ProcessingResult(
            df=stripped,
            summary=ProcessingSummary(
                name="strip_columns",
                description="Stripped Whitespace",
                data_modifications={"by_column": modifications},
            ),
        )

    def coerce_column_types(
        self,
        df: pd.DataFrame,
        column_types: dict[
            str, Literal["numeric", "integer", "bigint", "string", "date", "datetime"]
        ],
        errors: Literal["raise", "coerce"] = "raise",
    ) -> ProcessingResult:
        def to_str(obj):
            if isinstance(obj, int):
                return str(obj)
            elif isinstance(obj, float) and obj.is_integer():
                return str(int(obj))
            elif pd.isna(obj):
                return None
            else:
                return str(obj)

        result = df.copy()
        for column in column_types:
            match column_types[column]:
                case "numeric":
                    result[column] = pd.to_numeric(result[column], errors=errors)
                case "integer" | "bigint" as t:
                    mapping = {"integer": "Int32", "bigint": "Int64"}
                    result[column] = pd.array(result[column], dtype=mapping[t])  # type: ignore
                case "string":
                    # TODO: consider using .astype("string"), to avoid these columns having dtype `object`
                    result[column] = result[column].apply(to_str)
                case "date":
                    result[column] = pd.to_datetime(
                        result[column], errors=errors
                    ).dt.date
                    result[column] = result[column].replace(pd.NaT, None)  # type: ignore
                case "datetime":
                    result[column] = pd.to_datetime(result[column], errors=errors)
                    result[column] = result[column].replace(pd.NaT, None)  # type: ignore

        modified_cols = df.dtypes.sort_index() == result.dtypes.sort_index()
        modified = modified_cols.loc[~modified_cols].keys()

        return ProcessingResult(
            df=result,
            summary=ProcessingSummary(
                name="coerce_column_types",
                description="Coerced Column Types",
                column_modifications={
                    "modified": {
                        c: {"from": str(df[c].dtype), "to": str(result[c].dtype)}
                        for c in modified
                    }
                },  # TODO: evaluate if this is performant
            ),
        )

    # TODO
    def multi(self, df: gpd.GeoDataFrame) -> ProcessingResult:
        multi_gdf = df.copy()
        multi_gdf.set_geometry(
            gpd.GeoSeries([transform.multi(feature) for feature in multi_gdf.geometry]),
            inplace=True,
        )
        summary = make_generic_change_stats(
            df, multi_gdf, description="Converted geometries", name="multi"
        )
        return ProcessingResult(df=multi_gdf, summary=summary)

    def pd_series_func(
        self,
        df: pd.DataFrame,
        column_name: str,
        function_name: str,
        output_column_name: str | None = None,
        geo: bool = False,  # only used for validation
        **kwargs,
    ) -> ProcessingResult:
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
        summary = make_generic_change_stats(
            df,
            transformed,
            description=f"Applied {function_name} to column {column_name}",
            name="pd_series_func",
        )
        return ProcessingResult(df=transformed, summary=summary)


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
) -> list[ProcessingSummary]:
    """Validates and runs processing steps defined in config object"""
    logger.info(f"Processing {input_path.name} to {output_path.name}")
    df = geoparquet.read_df(input_path)
    compiled_steps = validate_processing_steps(dataset_id, processing_steps)

    logger.info("Running processing steps")
    summaries = []
    for step in compiled_steps:
        result = step(df)
        df = result.df
        summaries.append(result.summary)

    validate_columns(df, expected_columns)

    if output_csv:
        df.to_csv(output_path.parent / f"{dataset_id}.csv")

    df.to_parquet(output_path)

    return summaries
