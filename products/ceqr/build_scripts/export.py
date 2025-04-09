# export all data hub datasets which are avaiable to download
# - export DB tables as files (csvs, shapefiles, fgdbs)
# - copy non-DB data
import os
import shutil
import subprocess
import pandas as pd
import numpy as np

from dcpy.utils import postgres
from dcpy.utils.logging import logger

from .constants import BASH_UTILS_PATH, DATA_DIR, OUTPUT_DIR


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


def _export_dataset(row: pd.Series, pg_client: postgres.PostgresClient) -> bool:
    if row["availability_type"] == "webpage":
        return False

    if row["file_type"] == "excel":
        file_name = f"{row['dataset_id']}.xlsx"
        logger.info(
            f"For dataset {row['dataset_name']}, copying excel file {file_name}"
        )
        shutil.copy(DATA_DIR / file_name, OUTPUT_DIR / file_name)
        return True

    table_names = pg_client.get_schema_tables()
    if row["dataset_id"] not in table_names:
        logger.warning(
            f"Dataset '{row['dataset_name']}' ({row['dataset_id']}) is a TODO"
        )
        return False

    _export_dataset_from_db(
        dataset_name=row["dataset_name"],
        dataset_id=row["dataset_id"],
        file_type=row["file_type"],
        geometry_type=row["geometry_type"],
    )

    return True


def _add_download_link(
    dataset_id: str, availability_type: str, file_type: str, file_exists: bool
) -> str:
    digital_ocean_prefix = "https://nyc3.digitaloceanspaces.com/ceqr-data-hub/latest"
    if availability_type == "webpage":
        return "N/A"

    if not file_exists:
        return "download TODO"

    match file_type:
        case "csv":
            file_extension = "csv"
        case "shapefile":
            file_extension = "shp.zip"
            pass
        case "excel":
            file_extension = "xlsx"
        case _:
            raise ValueError(f"Unsupported file type '{file_type}'")
    filename = f"{dataset_id}.{file_extension}"
    url = os.path.join(digital_ocean_prefix, filename)
    text = f"[{file_type}]({url})"
    return text


def _add_link(url: str) -> str:
    if not url:
        return "N/A"
    text = f'<a href="{url}" target="_blank">webpage</a>'
    return text


def _export_datasets_table(
    datasets: pd.DataFrame, dataset_chapters: pd.DataFrame
) -> pd.DataFrame:
    file_path = OUTPUT_DIR / "datasets_table.md"

    datasets["Download Link"] = datasets.apply(
        lambda row: _add_download_link(
            row["dataset_id"],
            row["availability_type"],
            row["file_type"],
            row["file_exists"],
        ),
        axis=1,
    )
    datasets["Source Link"] = datasets.apply(
        lambda row: _add_link(row["source_url"]), axis=1
    )
    datasets = datasets.merge(dataset_chapters, on="dataset_name", how="left")

    column_renamings = {
        "dataset_name": "Dataset Name",
    }
    datasets_for_markdown = datasets.rename(
        column_renamings,
        axis=1,
    ).replace(np.nan, "")

    columns_to_drop = [
        "version",
        "dataset_id",
        "availability_type",
        "file_type",
        "geometry_type",
        "source_url",
        "file_exists",
    ]
    datasets_for_markdown = datasets_for_markdown.drop(columns=columns_to_drop)

    with open(file_path, "w") as f:
        datasets_for_markdown.to_markdown(
            buf=f,
            tablefmt="github",
            index=False,
        )
    logger.info(f"Generated markdown file {file_path}")
    return datasets_for_markdown


def _export_chapter_tables(
    chapter_datasets: pd.DataFrame, datasets: pd.DataFrame, chapters: pd.DataFrame
):
    chapter_datasets_columns = [
        "Category",
        "Dataset Name",
        "Download Link",
        "Source Link",
        "Use",
    ]
    column_renamings = {
        "use_category": "Category",
        "dataset_name": "Dataset Name",
        "use_details": "Use",
    }
    chapter_datasets = chapter_datasets.rename(
        column_renamings,
        axis=1,
    )
    chapter_datasets = chapter_datasets.merge(datasets, on="Dataset Name", how="left")

    file_path = OUTPUT_DIR / "chapters_tables.md"
    with open(file_path, "w") as f:
        for _, chapter in chapters.iterrows():
            chapter_id = chapter["chapter_id"]
            chapter_name = chapter["chapter_name"]
            chapter_df = chapter_datasets[chapter_datasets["chapter_id"] == chapter_id][
                chapter_datasets_columns
            ]
            chapter_heading = f"## {chapter_name}\r\r"
            f.write(chapter_heading)
            chapter_df.to_markdown(
                buf=f,
                tablefmt="github",
                index=False,
            )
            f.write("\r\r")
    logger.info(f"Generated markdown file {file_path}")


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
    dataset_versions["file_exists"] = dataset_versions.apply(
        lambda row: _export_dataset(row, pg_client), axis=1
    )

    # construct dataset chapters table
    chapter_datasets = pg_client.read_table_df("chapter_datasets")
    chapters = pg_client.read_table_df("chapters")

    chapter_datasets = chapter_datasets.merge(chapters, on="chapter_id", how="left")
    dataset_chapters = chapter_datasets.pivot(
        index="dataset_name", columns="chapter_name", values="dataset_name"
    )
    dataset_chapters = dataset_chapters[chapters["chapter_name"]]
    for chapter_name in chapters["chapter_name"]:
        dataset_chapters[chapter_name] = dataset_chapters[chapter_name].apply(
            lambda val: "X" if not pd.isnull(val) else val
        )

    # generate markdown tables for data hub github page
    datasets_for_markdown = _export_datasets_table(dataset_versions, dataset_chapters)
    _export_chapter_tables(chapter_datasets, datasets_for_markdown, chapters)


if __name__ == "__main__":
    export()
