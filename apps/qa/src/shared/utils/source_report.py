# functions used to generate source data reports
import pandas as pd
from typing import cast

from dcpy.connectors.edm import recipes, publishing
from src import QAQC_DB_SCHEMA_SOURCE_DATA
from src.shared.constants import construct_dataset_by_version, SQL_FILE_DIRECTORY


def dataframe_style_source_report_results(value) -> str:
    color = "rgba(0,155,0,.2)" if value else "rgba(155,0,0,.2)"
    return f"background-color: {color}"


def get_source_dataset_ids(product_key: publishing.ProductKey) -> list[str]:
    """Gets names of source products used in build
    TODO this should not come from publishing, but should be defined in code for each data product
    """
    source_data_versions = publishing.get_source_data_versions(product_key)
    return sorted(source_data_versions.index.values.tolist())


def get_source_data_versions_to_compare(
    reference_product_key: publishing.ProductKey,
    staging_product_key: publishing.ProductKey,
):
    # TODO (nice-to-have) add column with links to data-library yaml templates
    reference_source_data_versions = publishing.get_source_data_versions(
        reference_product_key
    )
    latest_source_data_versions = publishing.get_source_data_versions(
        staging_product_key
    )
    source_data_versions = reference_source_data_versions.merge(
        latest_source_data_versions,
        left_index=True,
        right_index=True,
        suffixes=("_reference", "_latest"),
    )
    source_data_versions.sort_index(ascending=True, inplace=True)

    return source_data_versions


def compare_source_data_columns(source_report_results: dict, pg_client) -> dict:
    for dataset_id in source_report_results:
        reference_table = construct_dataset_by_version(
            dataset_id, source_report_results[dataset_id]["version_reference"]
        )
        latest_table = construct_dataset_by_version(
            dataset_id, source_report_results[dataset_id]["version_latest"]
        )
        reference_columns = pg_client.get_table_columns(table_name=reference_table)
        latest_columns = pg_client.get_table_columns(table_name=latest_table)
        source_report_results[dataset_id]["same_columns"] = (
            reference_columns == latest_columns
        )
    return source_report_results


def compare_source_data_row_count(source_report_results: dict, pg_client) -> dict:
    for dataset_name in source_report_results:
        reference_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_reference"]
        )
        latest_table = construct_dataset_by_version(
            dataset_name, source_report_results[dataset_name]["version_latest"]
        )
        reference_row_count = pg_client.get_table_row_count(table_name=reference_table)
        latest_row_count = pg_client.get_table_row_count(table_name=latest_table)
        source_report_results[dataset_name]["same_row_count"] = (
            reference_row_count == latest_row_count
        )
    return source_report_results


def load_source_data_to_compare(
    dataset: str, source_data_versions: pd.DataFrame, pg_client
) -> list[str]:
    status_messages = []
    version_reference = cast(
        str, source_data_versions.loc[dataset]["version_reference"]
    )
    version_staging = cast(str, source_data_versions.loc[dataset]["version_latest"])
    print(f"⏳ Loading {dataset} ({version_reference}, {version_staging}) ...")
    for version in [version_reference, version_staging]:
        status_message = load_source_data(
            dataset_id=dataset, version=version, pg_client=pg_client
        )
        status_messages.append(status_message)

    return status_messages


def load_source_data(dataset_id: str, version: str, pg_client) -> str:
    dataset = recipes.Dataset(
        id=dataset_id, version=version, file_type=recipes.DatasetType.pg_dump
    )
    recipes.fetch_dataset(dataset, target_dir=SQL_FILE_DIRECTORY)

    dataset_by_version = construct_dataset_by_version(dataset.id, version)
    schema_tables = pg_client.get_schema_tables()

    status_message = None
    if dataset_by_version not in schema_tables:
        recipes.import_dataset(dataset, pg_client, local_library_dir=SQL_FILE_DIRECTORY)
        status_message = f"Loaded `{QAQC_DB_SCHEMA_SOURCE_DATA}.{dataset_by_version}`"
    else:
        status_message = (
            f"Database already has `{QAQC_DB_SCHEMA_SOURCE_DATA}.{dataset_by_version}`"
        )

    return status_message
