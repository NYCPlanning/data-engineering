# functions used to generate source data reports
import pandas as pd
from dcpy.connectors.edm import recipes, publishing
from dcpy.utils.postgres import (
    create_sql_schema,
    load_data_from_sql_dump,
    get_schemas,
    get_schema_tables,
    get_table_columns,
    get_table_row_count,
)
from src.constants import construct_dataset_by_version, SQL_FILE_DIRECTORY

from . import QAQC_DB_SCHEMA_SOURCE_DATA


def dataframe_style_source_report_results(value: bool):
    color = "rgba(0,155,0,.2)" if value else "rgba(155,0,0,.2)"
    return f"background-color: {color}"


def get_source_dataset_names(product_key: publishing.Product) -> pd.DataFrame:
    """Gets names of source products used in build
    TODO this should not come from publishing, but should be defined in code for each data product
    """
    return sorted(product_key.source_data_versions.index.values.tolist())


def get_latest_source_data_versions(product: str) -> pd.DataFrame:
    """Gets latest available versions of source datasets for specific data product
    Does NOT return versions used in any specific build
    """
    source_data_versions = publishing.get_latest_source_versions(product)
    source_data_versions["version"] = source_data_versions.index.map(
        recipes.get_latest_version
    )
    return source_data_versions


def get_source_data_versions_to_compare(
    reference_product_key: publishing.Product,
    staging_product_key: publishing.Product,
) -> pd.DataFrame:
    # TODO (nice-to-have) add column with links to data-library yaml templates
    source_data_versions = reference_product_key.source_data_versions.merge(
        staging_product_key.source_data_versions,
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
    recipes.fetch_sql(dataset, version, SQL_FILE_DIRECTORY)

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
