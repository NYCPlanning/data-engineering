import shutil
from pathlib import Path
import streamlit as st
from dcpy.utils import s3

BUCKET = "edm-recipes"


def archive_raw_data(dataset_name: str, version: str, uploaded_file, file_name: str):

    base_path = Path(dataset_name)
    file_path = base_path / dataset_name
    base_path.mkdir(parents=True, exist_ok=True)

    s3_path = Path("inbox") / dataset_name / version / f"{file_name}"
    st.success(f"Archive started...")

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(
        f"Temporary file {uploaded_file.name} saved successfully"
    )

    s3.upload_file(
        BUCKET,
        file_path,
        f"inbox/{dataset_name}/{version}/{file_name}",
        "public-read",
    )

    st.success(
        f"Dataset {dataset_name} version {version} has been archived successfully to {s3_path}."
    )

    try:
        shutil.rmtree(base_path)
        st.success(f"Local temporary files cleaned up successfully.")
    except Exception as e:
        st.error("No cleanup needed")

    st.success(f"Process completed.")
    return file_path
