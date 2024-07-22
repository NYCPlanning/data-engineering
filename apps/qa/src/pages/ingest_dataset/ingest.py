def ingest():
    import streamlit as st
    from .helpers import archive_raw_data
    from pathlib import Path

    st.title("Ingest Dataset")

    dataset_name = st.text_input("Dataset Name")
    version = st.text_input("Version")
    uploaded_file = st.file_uploader("Choose a file")
    s3_path = Path("inbox") / dataset_name / version
    file_name = uploaded_file.name

    if st.button("Ingest"):
        if dataset_name and version and uploaded_file:
            file_path = archive_raw_data(dataset_name, version, uploaded_file, file_name)
        else:
            st.warning("Please input all fields.")
