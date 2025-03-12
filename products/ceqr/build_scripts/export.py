# export all data hub datasets which are avaiable to download
# - export DB tables as files (csvs, shapefiles, fgdbs)
# - copy non-DB data
import shutil
import subprocess
import pandas as pd

from dcpy.utils.logging import logger

from . import BASH_UTILS_PATH, DATA_DIR, OUTPUT_DIR, PG_CLIENT


def _call_bash_function(function_name, *args) -> None:
    command = f"source {BASH_UTILS_PATH} && set_error_traps && {function_name} {' '.join(map(str, args))}"
    subprocess.run(command, shell=True, executable="/bin/bash")


def _export_dataset_from_db(row: pd.Series) -> None:
    table_name = row["dataset_id"]

    match row["file_type"]:
        case "csv":
            file_suffix = "csv"
            bash_args = ["csv_export", table_name]
        case "shapefile":
            file_suffix = "shp.zip"
            bash_args = ["shp_export", table_name, row["geometry_type"]]
        case _:
            raise Exception(f"Cannot export a table to {row['file_type']}")

    file_name = f"{table_name}.{file_suffix}"
    file_path_output = OUTPUT_DIR / file_name
    logger.info(
        f"For dataset {row['dataset_name']}, exporting table {table_name} to {file_path_output}"
    )
    _call_bash_function(*bash_args)
    shutil.move(file_name, file_path_output)


def _export_dataset(row: pd.Series) -> None:
    if row["file_type"] == "excel":
        file_name = f"{row['dataset_id']}.xlsx"
        logger.info(
            f"For dataset {row['dataset_name']}, copying excel file {file_name}"
        )
        shutil.copy(DATA_DIR / file_name, OUTPUT_DIR / file_name)
        pass
    else:
        _export_dataset_from_db(row)


def export():
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True)

    # copy build metadata
    shutil.copy("build_metadata.json", OUTPUT_DIR / "build_metadata.json")

    # load dataset versions table to a dataframe
    dataset_versions = PG_CLIENT.read_table_df("dataset_versions")

    # export dataset versions table
    dataset_versions_export_path = (OUTPUT_DIR / "dataset_versions").with_suffix(".csv")
    logger.info(f"Exporting dataset_versions to {dataset_versions_export_path}")
    dataset_versions.to_csv(dataset_versions_export_path, index=False)

    # for each dataset, export the table as the specified file format
    for _, row in dataset_versions.iterrows():
        if row["availability_type"] == "download":
            _export_dataset(row)


if __name__ == "__main__":
    export()
