from pathlib import Path
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile
from time import sleep

from dcpy.utils import s3
from dcpy.library.archive import Archive

BUCKET = "edm-recipes"


def archive_raw_data(
    dataset_name: str, version: str, uploaded_file: UploadedFile, file_name: str, allow_override: bool
) -> Path:

    base_path = Path(".library") / "upload"
    file_path = base_path / file_name
    base_path.mkdir(parents=True, exist_ok=True)

    s3_path = Path("inbox") / dataset_name / version / f"{file_name}"

    exists = s3.exists(BUCKET,str(s3_path))
    if exists == True:
        st.error("Warning: File path already exists")
        if allow_override == False:
            st.error("File already exists on S3. Check the allow override box if you wish to continue")       
            raise FileExistsError("S3 Path/File Exists")
            
    try:
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
    except Exception as e:
        st.error("Failed to save temporary file: {e}")

    try:
        s3.upload_file(
            BUCKET,
            file_path,
            f"inbox/{dataset_name}/{version}/{file_name}",
            "public-read",
        )
    except Exception as e:
        st.error(
            "Failed to archive Dataset {dataset_name} version {version} to {s3_path}: {e}"
        )
    return file_path


def library_archive(
    dataset_name: str, version: str, s3_path: str, latest: bool
) -> None:
    a = Archive()
    # once we've tested and this is ready to go, need to add `push=True`
    a(name=dataset_name, version=version, override_path=s3_path, latest=latest)


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
