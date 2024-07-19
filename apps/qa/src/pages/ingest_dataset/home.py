# apps/qa/ingest_dataset.py
import streamlit as st
from helper import handle_file_upload


st.title("Ingest Dataset")

dataset_name = st.text_input("Dataset Name")
version = st.text_input("Version")
uploaded_file = st.file_uploader("Choose a file")

if st.button("Ingest"):
    if dataset_name and version and uploaded_file:
        file_path = handle_file_upload(dataset_name, version, uploaded_file)
    else:
        st.warning("Please input all fields.")

