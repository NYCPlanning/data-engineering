
# apps/qa/helper.py
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
import streamlit as st
from dcpy.lifecycle.ingest import configure
from dcpy.connectors.edm import recipes
from dcpy.utils import s3
import yaml

BUCKET = "edm-recipes"

def handle_file_upload(dataset_name: str, version: str, uploaded_file):
    base_path = Path(dataset_name) / version
    base_path.mkdir(parents=True, exist_ok=True)
    
    file_path = base_path / uploaded_file.name
    s3_path = Path('inbox')/dataset_name/version
    st.success(f"Archive started...")

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(f"Temporary file {uploaded_file.name} saved successfully to {file_path}")

    config = configure.get_config(dataset_name, version)

    recipes._archive_dataset(config, file_path, s3_path)

    st.success(f"Dataset {dataset_name} version {version} has been archived successfully to {s3_path}.")

    shutil.rmtree(base_path.parent)
    shutil.rmtree(Path("__pycache__"))
    st.success(f"Local temporary files and pycache deleted successfully.")

    st.success(f"Process completed.")

    
    return file_path
