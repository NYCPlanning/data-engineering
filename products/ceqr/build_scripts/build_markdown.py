import os
import shutil

import numpy as np
import pandas as pd

from dcpy.utils import postgres
from dcpy.utils.logging import logger

from .constants import DATA_DIR


def _add_download_link(
    dataset_id: str,
    availability_type: str,
    file_type: str,
    pg_client: postgres.PostgresClient,
) -> str:
    digital_ocean_prefix = "https://nyc3.digitaloceanspaces.com/ceqr-data-hub/latest"
    if availability_type == "webpage":
        return "N/A"

    table_names = pg_client.get_schema_tables()
    if dataset_id not in table_names and file_type != "excel":
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


def build_chapter_datasets() -> pd.DataFrame:
    pg_client = postgres.PostgresClient()
    chapters = pg_client.read_table_df("chapters")
    chapter_datasets = pg_client.read_table_df("chapter_datasets")

    chapter_datasets = chapter_datasets.merge(chapters, on="chapter_id", how="left")

    return chapter_datasets


def build_datasets_table_markdown(chapter_datasets: pd.DataFrame) -> pd.DataFrame:
    file_path = DATA_DIR / "datasets_table.md"

    pg_client = postgres.PostgresClient()
    chapters = pg_client.read_table_df("chapters")
    datasets = pg_client.read_table_df("dataset_versions")

    # dataset can appear more than once under a chapter but with different use_category
    chapter_datasets_unique = chapter_datasets[
        ["dataset_name", "chapter_name"]
    ].drop_duplicates()

    dataset_chapters = chapter_datasets_unique.pivot(
        index="dataset_name", columns="chapter_name", values="dataset_name"
    )
    for chapter_name in chapters["chapter_name"]:
        dataset_chapters.rename(
            columns={chapter_name: f"{chapter_name}_val"}, inplace=True
        )
        dataset_chapters[chapter_name] = dataset_chapters[f"{chapter_name}_val"].apply(
            lambda val: "X" if not pd.isnull(val) else val
        )
        dataset_chapters.drop(columns=[f"{chapter_name}_val"], inplace=True)

    datasets["Download Link"] = datasets.apply(
        lambda row: _add_download_link(
            row["dataset_id"], row["availability_type"], row["file_type"], pg_client
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
    datasets_for_markdown = datasets.rename(columns=column_renamings)
    datasets_for_markdown = datasets_for_markdown.replace(np.nan, "")

    columns_to_drop = [
        "version",
        "dataset_id",
        "availability_type",
        "file_type",
        "geometry_type",
        "source_url",
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


def build_chapter_tables_markdown(
    chapter_datasets: pd.DataFrame, datasets: pd.DataFrame
):
    file_path = DATA_DIR / "chapters_tables.md"
    pg_client = postgres.PostgresClient()
    chapters = pg_client.read_table_df("chapters")

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
    chapter_datasets = chapter_datasets.rename(columns=column_renamings)
    chapter_datasets = chapter_datasets.merge(datasets, on="Dataset Name", how="left")

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


if __name__ == "__main__":
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
    DATA_DIR.mkdir(parents=True)

    chapter_datasets = build_chapter_datasets()
    datasets = build_datasets_table_markdown(chapter_datasets)
    build_chapter_tables_markdown(chapter_datasets, datasets)
