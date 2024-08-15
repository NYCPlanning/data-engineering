from pathlib import Path
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile
from time import sleep

from dcpy.utils import s3
from dcpy.library.archive import Archive
from io import BytesIO


BUCKET = "edm-recipes"


def archive_raw_data(
    dataset_name: str,
    version: str,
    uploaded_file: UploadedFile,
    file_name: str,
    allow_override: bool,
) -> None:

    s3_path = f"inbox/{dataset_name}/{version}/{file_name}"

    if s3.exists(BUCKET, s3_path) and not allow_override:
        raise FileExistsError(
            "File already exists on S3. Check the allow override box if you wish to continue"
        )

    file_obj = BytesIO(uploaded_file.read())

    s3.upload_file_obj(
        file_obj,
        BUCKET,
        s3_path,
        "public-read",
    )


def library_archive(
    dataset_name: str, version: str, s3_path: str, latest: bool
) -> None:
    a = Archive()
    # once we've tested and this is ready to go, need to add `push=True`
    a(
        clean=True,
        latest=latest,
        name=dataset_name,
        source_path_override=f"s3://{BUCKET}/{s3_path}",
        version=version,
    )
