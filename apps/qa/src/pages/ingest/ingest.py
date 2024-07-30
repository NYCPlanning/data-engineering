def ingest():
    import streamlit as st
    from .helpers import archive_raw_data
    from pathlib import Path
    import time

    if "ingest" not in st.session_state:
        st.session_state["ingest"] = {
            "running": False,
            "ingest_status": None
        }

    def run_start():
        if (
            dataset_name
            and version
            and uploaded_file
        ):
            st.session_state["ingest"]["running"] = True
        else:
            st.warning("Please input all fields.")

    def start_ingest():
        with st.spinner("Ingesting"):
            try:
                file_path = archive_raw_data(
                    dataset_name,
                    version,
                    uploaded_file,
                    uploaded_file.name
                )
                st.session_state["ingest"]["ingest_status"] = "success"
            except Exception as e:
                st.session_state["ingest"]["ingest_status"] = "fail"
                ingest_error_message = e

    def run_stop():
        st.session_state["ingest"]["running"] = False
        st.session_state["ingest"]["ingest_status"] = None


    st.title("Ingest Dataset")

    dataset_name = st.text_input(
        "Dataset Name",
        disabled=st.session_state["ingest"]["running"],
    )
    version = st.text_input(
        "Version",
        disabled=st.session_state["ingest"]["running"],
    )
    uploaded_file = st.file_uploader(
        "Choose a file", 
        disabled=st.session_state["ingest"]["running"]
    )

    if dataset_name and version:
        s3_path = (
                Path("inbox")
                / dataset_name
                / version
            )
    
    ingest_button_pressed  = st.button("Ingest", on_click=run_start, disabled=st.session_state["ingest"]["running"])
    
    if ingest_button_pressed == True and st.session_state["ingest"]["running"] == True:
        start_ingest()
    if st.session_state["ingest"]["ingest_status"] == "success":
        st.success("Ingest Successful")
        st.button("Restart", on_click=run_stop)
    if st.session_state["ingest"]["ingest_status"] == "fail":
        st.error(ingest_error_message)
        st.button("Restart", on_click=run_stop)
