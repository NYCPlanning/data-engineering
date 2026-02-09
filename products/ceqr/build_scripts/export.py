# export all data hub datasets which are avaiable to download
# - export DB tables as files (csvs, shapefiles, fgdbs)
# - copy non-DB data
import shutil
import subprocess

import pandas as pd

from dcpy.utils import postgres
from dcpy.utils.logging import logger

from .constants import BASH_UTILS_PATH, DATA_DIR, DATASETS_DIR, OUTPUT_DIR


def _call_bash_function(function_name, *args) -> None:
    command = f"source {BASH_UTILS_PATH} && set_error_traps && {function_name} {' '.join(map(str, args))}"
    subprocess.run(command, shell=True, executable="/bin/bash")


def _export_dataset_from_db(
    dataset_name: str,
    dataset_id: str,
    file_type: str,
    geometry_type: str,
) -> None:
    table_name = dataset_id

    match file_type:
        case "csv":
            file_suffix = "csv"
            bash_args = ["csv_export", table_name]
        case "shapefile":
            file_suffix = "shp.zip"
            bash_args = ["shp_export", table_name, geometry_type]
        case _:
            raise Exception(f"Cannot export a table to {file_type}")

    file_name = f"{table_name}.{file_suffix}"
    file_path = OUTPUT_DIR / file_name
    logger.info(
        f"For dataset {dataset_name}, exporting table {table_name} to {file_path} ..."
    )
    _call_bash_function(*bash_args)
    shutil.move(file_name, file_path)


def _export_dataset(row: pd.Series, pg_client: postgres.PostgresClient):
    if row["availability_type"] == "webpage":
        return

    if row["file_type"] == "excel":
        file_name = f"{row['dataset_id']}.xlsx"
        logger.info(
            f"For dataset {row['dataset_name']}, copying excel file {file_name}"
        )
        shutil.copy(DATASETS_DIR / file_name, OUTPUT_DIR / file_name)
        return

    table_names = pg_client.get_schema_tables()
    if row["dataset_id"] not in table_names:
        logger.warning(
            f"Dataset '{row['dataset_name']}' ({row['dataset_id']}) is a TODO"
        )
        return

    _export_dataset_from_db(
        dataset_name=row["dataset_name"],
        dataset_id=row["dataset_id"],
        file_type=row["file_type"],
        geometry_type=row["geometry_type"],
    )


def export():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    pg_client = postgres.PostgresClient()

    # copy build metadata
    shutil.copy("build_metadata.json", OUTPUT_DIR / "build_metadata.json")

    # export dataset versions table
    dataset_versions = pg_client.read_table_df("dataset_versions")
    dataset_versions_export_path = (OUTPUT_DIR / "dataset_versions").with_suffix(".csv")
    logger.info(f"Exporting dataset_versions to {dataset_versions_export_path}")
    dataset_versions.to_csv(dataset_versions_export_path, index=False)

    # export dataset files
    dataset_versions.apply(lambda row: _export_dataset(row, pg_client), axis=1)

    # export markdown files
    shutil.copy(DATA_DIR / "datasets_table.md", OUTPUT_DIR / "datasets_table.md")
    shutil.copy(DATA_DIR / "chapters_tables.md", OUTPUT_DIR / "chapters_tables.md")


if __name__ == "__main__":
    export()
