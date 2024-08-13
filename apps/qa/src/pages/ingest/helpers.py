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

    s3_path = Path("inbox") / dataset_name / version / f"{file_name}"

    exists = s3.exists(BUCKET, str(s3_path))
    if exists == True and allow_override == False:
        raise FileExistsError(
            "File already exists on S3. Check the allow override box if you wish to continue"
        )

    file_obj = BytesIO(uploaded_file.read())

    s3.upload_file_obj(
        file_obj,
        BUCKET,
        f"inbox/{dataset_name}/{version}/{file_name}",
        "public-read",
    )


def library_archive(
    dataset_name: str, version: str, s3_path: str, latest: bool
) -> None:
    a = Archive()
    # once we've tested and this is ready to go, need to add `push=True`
    a(
        name=dataset_name,
        version=version,
        override_path=s3_path,
        latest=latest,
        clean=True,
    )


def dummy_archive_raw_data(
    dataset_name: str, version: str, uploaded_file: UploadedFile, file_name: str
) -> str | None:
    sleep(5)

    if dataset_name == "error":
        return None
    else:
        return "dummy_path"


def dummy_library_call(dataset_name: str, version: str, s3_path: Path) -> str | None:
    sleep(5)

    if version == "error":
        return None
    else:
        return "dummy_path"
