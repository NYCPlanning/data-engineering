# functions used to generate source data reports
import pandas as pd
import urllib
from src.constants import construct_dataset_by_version, DATASET_SOURCE_VERSIONS
from src.digital_ocean_utils import (
    construct_output_data_directory_url,
    get_datatset_config,
    get_data_library_sql_file,
)

from . import QAQC_DB_SCHEMA_SOURCE_DATA
from dcpy.connectors.postgres import (
    create_sql_schema,
    load_data_from_sql_dump,
    get_schemas,
    get_schema_tables,
    get_table_columns,
    get_table_row_count,
)


def dataframe_style_source_report_results(value: bool):
    color = "rgba(0,155,0,.2)" if value else "rgba(155,0,0,.2)"
    return f"background-color: {color}"


def get_source_dataset_names(dataset: str, version: str) -> pd.DataFrame:
    source_data_versions = get_source_data_versions_from_build(
        dataset=dataset, version=version
    )
    return sorted(source_data_versions.index.values.tolist())


def get_source_data_versions_from_build(dataset: str, version: str) -> pd.DataFrame:
    try:
        source_data_versions = pd.read_csv(
            f"{construct_output_data_directory_url(dataset=dataset, version=version)}/source_data_versions.csv",
            index_col=False,
            dtype=str,
        )
    except urllib.error.HTTPError:
        # TODO handle lack of source_data_versions file
        # NOTE this uses a placeholder for an un-implemented feature
        source_data_versions = DATASET_SOURCE_VERSIONS[dataset]
    source_data_versions.rename(
        columns={
            "schema_name": "datalibrary_name",
            "v": "version",
        },
        errors="raise",
        inplace=True,
    )
    source_data_versions.sort_values(
        by=["datalibrary_name"], ascending=True
    ).reset_index(drop=True, inplace=True)
    source_data_versions.set_index("datalibrary_name", inplace=True)
    return source_data_versions


def get_latest_source_data_versions(dataset: str) -> pd.DataFrame:
    source_data_versions = get_source_data_versions_from_build(
        dataset=dataset, version="latest"
    )
    source_data_versions["version"] = source_data_versions.index.map(
        lambda dataset: get_datatset_config(
            dataset=dataset,
            version="latest",
        )[
            "dataset"
        ]["version"]
    )
    return source_data_versions


def get_source_data_versions_to_compare(
    dataset: str, reference_version: str, staging_version: str
):
    # TODO (nice-to-have) add column with links to data-library yaml templates
    reference_source_data_versions = get_source_data_versions_from_build(
        dataset=dataset, version=reference_version
    )
    if not staging_version:
        latest_source_data_versions = get_latest_source_data_versions(dataset=dataset)
    else:
        latest_source_data_versions = get_source_data_versions_from_build(
            dataset=dataset, version=staging_version
        )
    source_data_versions = reference_source_data_versions.merge(
        latest_source_data_versions,
        left_index=True,
        right_index=True,
        suffixes=("_reference", "_latest"),
    )
    source_data_versions.sort_index(ascending=True, inplace=True)

    return source_data_versions


def compare_source_data_columns(source_report_results: dict) -> dict:
    for dataset_name in source_report_results:
        reference_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_reference"]
        )
        latest_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_latest"]
        )
        reference_columns = get_table_columns(
            table_schema=QAQC_DB_SCHEMA_SOURCE_DATA, table_name=reference_table
        )
        latest_columns = get_table_columns(
            table_schema=QAQC_DB_SCHEMA_SOURCE_DATA, table_name=latest_table
        )
        source_report_results[dataset_name]["same_columns"] = (
            reference_columns == latest_columns
        )
    return source_report_results


def compare_source_data_row_count(source_report_results: dict) -> dict:
    for dataset_name in source_report_results:
        reference_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_reference"]
        )
        latest_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_latest"]
        )
        reference_row_count = get_table_row_count(
            table_schema=QAQC_DB_SCHEMA_SOURCE_DATA, table_name=reference_table
        )
        latest_row_count = get_table_row_count(
            table_schema=QAQC_DB_SCHEMA_SOURCE_DATA, table_name=latest_table
        )
        source_report_results[dataset_name]["same_row_count"] = (
            reference_row_count == latest_row_count
        )
    return source_report_results


def create_source_data_schema() -> None:
    schema_names = get_schemas()
    if QAQC_DB_SCHEMA_SOURCE_DATA not in schema_names:
        schema_names = create_sql_schema(table_schema=QAQC_DB_SCHEMA_SOURCE_DATA)
    print("DEV schemas in DB EDM_DATA/edm-qaqc:")
    print(f"{schema_names}")


# def load_all_source_data(
#     dataset_names: list[str], source_data_versions: pd.DataFrame
# ) -> list:
#     pool = multiprocessing.Pool(processes=4)
#     pool.starmap(
#         load_source_data_to_compare,
#         zip(dataset_names, itertools.repeat(source_data_versions)),
#     )
#     table_names = get_schema_tables(table_schema=DATASET_QAQC_DB_SCHEMA)
#     return table_names


def load_source_data_to_compare(
    dataset: str, source_data_versions: pd.DataFrame
) -> list[str]:
    status_messages = []
    version_reference = source_data_versions.loc[dataset, "version_reference"]
    version_staging = source_data_versions.loc[dataset, "version_latest"]
    print(f"⏳ Loading {dataset} ({version_reference}, {version_staging}) ...")
    for version in [version_reference, version_staging]:
        status_message = load_source_data(dataset=dataset, version=version)
        status_messages.append(status_message)

    return status_messages


def load_source_data(dataset: str, version: str) -> str:
    get_data_library_sql_file(dataset=dataset, version=version)

    dataset_by_version = construct_dataset_by_version(dataset, version)
    schema_tables = get_schema_tables(table_schema=QAQC_DB_SCHEMA_SOURCE_DATA)

    status_message = None
    if not dataset_by_version in schema_tables:
        load_data_from_sql_dump(
            table_schema=QAQC_DB_SCHEMA_SOURCE_DATA,
            dataset_by_version=dataset_by_version,
            dataset_name=dataset,
        )
        status_message = f"Loaded `{QAQC_DB_SCHEMA_SOURCE_DATA}.{dataset_by_version}`"
    else:
        status_message = (
            f"Database already has `{QAQC_DB_SCHEMA_SOURCE_DATA}.{dataset_by_version}`"
        )

    return status_message
