import os

from dcpy.utils.s3 import client

from . import (
    CORRECTIONS_DATA_PATH,
    DCP_HOUSING_CORRECTIONS_FILENAMES,
    DCP_HOUSING_DATA_FILENAMES,
    RAW_DATA_PATH,
    S3_BUCKET,
    S3_SOURCE_HOUSING_TEAM_DIR,
)


def download_s3_source_file(file_name: str) -> None:
    s3_filepath = f"{S3_SOURCE_HOUSING_TEAM_DIR}/raw/{file_name}"
    save_file_path = f"{RAW_DATA_PATH}/{file_name}"
    client().download_file(S3_BUCKET, s3_filepath, save_file_path)


def download_s3_corrections_file(dataset: str, file_name: str) -> None:
    s3_filepath = f"{S3_SOURCE_HOUSING_TEAM_DIR}/corrections/{file_name}"
    save_file_path = f"{CORRECTIONS_DATA_PATH}/{dataset}.csv"
    client().download_file(S3_BUCKET, s3_filepath, save_file_path)


if __name__ == "__main__":
    print("Downloading DCP Housing team source data ...")

    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    for dataset, filename in DCP_HOUSING_DATA_FILENAMES.items():
        print(f"downloading source dataset '{dataset}' from filename '{filename}' ...")
        download_s3_source_file(filename)

    os.makedirs(CORRECTIONS_DATA_PATH, exist_ok=True)
    for dataset, filename in DCP_HOUSING_CORRECTIONS_FILENAMES.items():
        print(f"downloading corrections '{dataset}' from filename '{filename}' ...")
        download_s3_corrections_file(dataset, filename)

    print("Done!")
