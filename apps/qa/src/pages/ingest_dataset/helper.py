import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
import streamlit as st
from dcpy.lifecycle.ingest import configure
from dcpy.connectors.edm import recipes
from dcpy.utils import s3
import yaml
from dcpy.library.archive import Archive
import os

BUCKET = "edm-recipes"


def archive_raw_data(dataset_name: str, version: str, uploaded_file, file_name):
    base_path = Path(dataset_name) / version
    base_path.mkdir(parents=True, exist_ok=True)

    file_path = base_path / dataset_name
    s3_path = Path("inbox") / dataset_name / version / f"{file_name}"
    st.success(f"Archive started...")

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success(f"Temporary file {uploaded_file.name} saved successfully to {file_path}")

    config = configure.get_config(dataset_name, version)

    recipes._archive_dataset(config, file_path, s3_path)

    st.success(
        f"Dataset {dataset_name} version {version} has been archived successfully to {s3_path}."
    )

    try:
        shutil.rmtree(base_path.parent)
        shutil.rmtree(Path("__pycache__"))
        st.success(f"Local temporary files and pycache deleted successfully.")
    except Exception as e:
        st.error("no deletion needed")

    st.success(f"Process completed.")
    return file_path


def edit_tamplate(dataset_name, s3_path):
    st.warning(f"Current working directory: {os.getcwd()}")
    template_path = (
        Path("../../../../../dcpy/library/templates") / f"{dataset_name}.yml"
    )
    with open(template_path, "r") as temp:
        template = yaml.safe_load(temp)
    new_tamplate_path = Path(f"{dataset_name}.yml")
    s3_path = str(s3_path)

    template["dataset"]["source"]["script"]["path"] = f"s3://{BUCKET}/{s3_path}"
    with open(new_tamplate_path, "w") as temp:
        yaml.safe_dump(template, temp)
    return new_tamplate_path


def library_archive(template_path):
    a = Archive()
    a(template_path, output_format="csv", push=True, clean=True, latest=True)
    return null


#### error: bucket not found
