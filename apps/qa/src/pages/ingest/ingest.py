def ingest():
    import streamlit as st
    from .helpers import archive_raw_data
    from pathlib import Path
    import time

    if "ingest" not in st.session_state:
        st.session_state["ingest"] = {
            "running": False,
            "dataset_name": None,
            "version": None,
            "s3_path": None,
            "uploaded_file": None,
            "ingest_status": None,
            "ingest_error_message": None,
        }

    def run_start():
        if (
            st.session_state["ingest"]["dataset_name"]
            and st.session_state["ingest"]["version"]
            and st.session_state["ingest"]["uploaded_file"]
        ):
            st.session_state["ingest"]["running"] = True
        else:
            st.warning("Please input all fields.")

    def start_ingest():
        with st.spinner("Ingesting"):
            try:
                file_path = archive_raw_data(
                    st.session_state["ingest"]["dataset_name"],
                    st.session_state["ingest"]["version"],
                    st.session_state["ingest"]["uploaded_file"],
                    st.session_state["ingest"]["uploaded_file"].name,
                )
                st.session_state["ingest"]["ingest_status"] = "success"
            except Exception as e:
                st.session_state["ingest"]["ingest_status"] = "fail"
                st.session_state["ingest"]["ingest_error_message"] = e

    def run_stop():
        st.session_state["ingest"]["running"] = False
        st.session_state["ingest"]["ingest_status"] = None
        st.session_state["ingest"]["ingest_error_message"] = None

    st.title("Ingest Dataset")
    st.session_state["ingest"]["dataset_name"] = st.text_input(
        "Dataset Name",
        value=st.session_state["ingest"]["dataset_name"],
        disabled=st.session_state["ingest"]["running"],
    )
    st.session_state["ingest"]["version"] = st.text_input(
        "Version",
        value=st.session_state["ingest"]["version"],
        disabled=st.session_state["ingest"]["running"],
    )
    st.session_state["ingest"]["uploaded_file"] = st.file_uploader(
        "Choose a file", disabled=st.session_state["ingest"]["running"]
    )
    if (
        st.session_state["ingest"]["dataset_name"]
        and st.session_state["ingest"]["version"]
    ):
        st.session_state["ingest"]["s3_path"] = (
            Path("inbox")
            / st.session_state["ingest"]["dataset_name"]
            / st.session_state["ingest"]["version"]
        )
    if (
        st.button(
            "Ingest", on_click=run_start, disabled=st.session_state["ingest"]["running"]
        )
        and st.session_state["ingest"]["running"] == True
    ):
        start_ingest()
    if st.session_state["ingest"]["ingest_status"] == "success":
        st.success("Ingest Successful")
        st.button("Restart", on_click=run_stop)
    if st.session_state["ingest"]["ingest_status"] == "fail":
        st.error(st.session_state["ingest"]["ingest_error_message"])
        st.button("Restart", on_click=run_stop)
