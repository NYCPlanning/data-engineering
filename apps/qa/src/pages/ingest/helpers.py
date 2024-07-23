import shutil
from pathlib import Path
import streamlit as st
from dcpy.utils import s3
from streamlit.runtime.uploaded_file_manager import UploadedFile

BUCKET = "edm-recipes"


def archive_raw_data(
    dataset_name: str, version: str, uploaded_file: UploadedFile, file_name: str
) -> Path:

    base_path = Path(".library") / "upload"
    file_path = base_path / file_name
    base_path.mkdir(parents=True, exist_ok=True)

    s3_path = Path("inbox") / dataset_name / version / f"{file_name}"

    st.success(f"Archive started...")

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(f"Temporary file {uploaded_file.name} saved successfully")

    s3.upload_file(
        BUCKET,
        file_path,
        f"inbox/{dataset_name}/{version}/{file_name}",
        "public-read",
    )

    st.success(
        f"Dataset {dataset_name} version {version} has been archived successfully to {s3_path}."
    )

    st.success(f"Process completed.")
    return file_path
