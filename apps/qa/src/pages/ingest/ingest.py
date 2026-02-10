def ingest() -> None:
    import streamlit as st
    from streamlit.runtime.uploaded_file_manager import UploadedFile

    from dcpy.library import utils

    from .helpers import (
        archive_raw_data,
        library_archive,
    )

    if "ingest" not in st.session_state:
        st.session_state["ingest"] = {
            "running": False,
            "upload_status": None,
            "library_status": None,
            "error_message": None,
            "retry": False,
            "s3_path": None,
        }

    def lock_for_ingest(dataset_name: str, version: str) -> None:
        if dataset_name and version:
            st.session_state["ingest"]["running"] = True
        else:
            st.warning("Please input all fields.")

    def lock_for_library(dataset_name: str, version: str, s3_path: str) -> None:
        if dataset_name and version and s3_path:
            st.session_state["ingest"]["running"] = True
        else:
            st.warning("Please input all fields.")

    def ingest(
        dataset_name: str,
        version: str,
        uploaded_file: UploadedFile,
        allow_override: bool,
    ) -> None:
        with st.spinner("Ingesting"):
            try:
                archive_raw_data(
                    dataset_name,
                    version,
                    uploaded_file,
                    uploaded_file.name,
                    allow_override,
                )
                st.session_state["ingest"]["upload_status"] = "success"
            except Exception as e:
                st.error("Failed to archive Dataset")
                st.session_state["ingest"]["upload_status"] = "fail"
                st.session_state["ingest"]["error_message"] = str(e)

    def library(dataset_name: str, version: str, s3_path: str, latest: bool) -> None:
        with st.spinner("Calling Library"):
            try:
                library_archive(dataset_name, version, s3_path, latest)
                st.session_state["ingest"]["library_status"] = "success"
            except Exception as e:
                st.session_state["ingest"]["library_status"] = "fail"
                st.session_state["ingest"]["error_message"] = str(e)
            finally:
                st.session_state["ingest"]["running"] = False

    def retry() -> None:
        st.session_state["ingest"]["upload_status"] = None
        st.session_state["ingest"]["library_status"] = None
        st.session_state["ingest"]["error_message"] = None
        st.session_state["ingest"]["retry"] = True

    def end_retry() -> None:
        st.session_state["ingest"]["retry"] = False
        st.session_state["ingest"]["s3_path"] = None

    def unlock() -> None:
        st.session_state["ingest"]["running"] = False
        st.session_state["ingest"]["upload_status"] = None
        st.session_state["ingest"]["library_status"] = None
        st.session_state["ingest"]["error_message"] = None
        st.session_state["ingest"]["s3_path"] = None

    st.title("Ingest Dataset")

    process = st.sidebar.selectbox(
        "Choose a process:",
        (
            "Upload File and Call Library",
            "Upload Raw Files to S3",
            "Call Library on S3 Files",
        ),
        disabled=st.session_state["ingest"]["running"],
    )

    if st.session_state["ingest"]["retry"]:
        process = "Call Library on S3 Files"
        st.write(
            "You are being re-directed to retry library calls, we autofilled the s3 path to your saved file"
        )
        st.write("To quit retry mode, click the button below")

    if process == "Upload Raw Files to S3":
        st.write("Ingest Raw Files to S3")

        dataset_name = str(
            st.selectbox(
                "Choose a Dataset:",
                (utils.get_all_templates()),
                index=None,
                disabled=st.session_state["ingest"]["running"],
            )
        )

        version = str(
            st.text_input("Version", disabled=st.session_state["ingest"]["running"])
        )
        uploaded_file = st.file_uploader(
            "Choose a file", disabled=st.session_state["ingest"]["running"]
        )

        allow_override = st.checkbox("Allow Path/File Override")

        if dataset_name and version:
            s3_path_display = f"inbox / {dataset_name} / {version}"
            st.write("To S3 Directory:", s3_path_display)

        ingest_button_pressed = st.button(
            "Ingest",
            on_click=lock_for_ingest,
            args=(dataset_name, version),
            disabled=st.session_state["ingest"]["running"],
        )

        if (
            ingest_button_pressed
            and st.session_state["ingest"]["running"]
            and uploaded_file is not None
        ):
            ingest(dataset_name, version, uploaded_file, allow_override)
        if ingest_button_pressed and uploaded_file is None:
            st.error("Please upload a valid file")
            st.button("Dismiss", on_click=unlock)
        if st.session_state["ingest"]["upload_status"] == "success":
            st.success("Ingest Successful")
            st.button("Restart", on_click=unlock)
        if st.session_state["ingest"]["upload_status"] == "fail":
            st.error(st.session_state["ingest"]["error_message"])
            st.button("Restart", on_click=unlock)

    if process == "Call Library on S3 Files":
        st.write("Call Library on S3 Files")
        if st.session_state["ingest"]["retry"]:
            st.button(
                "End Retry",
                on_click=end_retry,
                disabled=st.session_state["ingest"]["running"],
            )
        dataset_name = str(
            st.selectbox(
                "Choose a Dataset:",
                (utils.get_all_templates()),
                index=None,
                disabled=st.session_state["ingest"]["running"],
            )
        )
        version = str(
            st.text_input("Version", disabled=st.session_state["ingest"]["running"])
        )
        s3_path = st.text_input(
            "S3 File Path (Default Bucket edm-recipes)",
            value=st.session_state["ingest"]["s3_path"],
            disabled=st.session_state["ingest"]["running"],
        )

        latest = st.checkbox("Tag as Latest Version")

        library_button_pressed = st.button(
            "Call Library",
            on_click=lock_for_library,
            args=(dataset_name, version, s3_path),
            disabled=st.session_state["ingest"]["running"],
        )
        if library_button_pressed and st.session_state["ingest"]["running"]:
            library(dataset_name, version, s3_path, latest)
        if st.session_state["ingest"]["library_status"] == "success":
            st.success("Ingest Successful")
            st.button("Restart", on_click=unlock)
        if st.session_state["ingest"]["library_status"] == "fail":
            st.error(st.session_state["ingest"]["error_message"])
            if st.session_state["ingest"]["retry"]:
                st.button("Retry", on_click=retry)
            else:
                st.button("Restart", on_click=unlock)

    if process == "Upload File and Call Library":
        st.write("Ingest and Call Library")
        dataset_name = str(
            st.selectbox(
                "Choose a Dataset:",
                (utils.get_all_templates()),
                index=None,
                disabled=st.session_state["ingest"]["running"],
            )
        )
        version = str(
            st.text_input("Version", disabled=st.session_state["ingest"]["running"])
        )

        uploaded_file = st.file_uploader(
            "Choose a file", disabled=st.session_state["ingest"]["running"]
        )

        allow_override = st.checkbox("Allow Path/File Override")

        latest = st.checkbox("Tag as Latest Version")

        if dataset_name and version:
            s3_path_display = f"inbox / {dataset_name} / {version}"
            st.write("To S3 Directory:", s3_path_display)

        ingest_button_pressed = False
        ingest_button_pressed = st.button(
            "Ingest and Call Library",
            on_click=lock_for_ingest,
            args=(dataset_name, version),
            disabled=st.session_state["ingest"]["running"],
        )
        if (
            ingest_button_pressed
            and st.session_state["ingest"]["running"]
            and uploaded_file is not None
        ):
            ingest(dataset_name, version, uploaded_file, allow_override)
            s3_path = f"inbox/{dataset_name}/{version}/{uploaded_file.name}"
        if ingest_button_pressed and uploaded_file is None:
            st.error("Please upload a valid file")
            st.button("Dismiss", on_click=unlock)
        if st.session_state["ingest"]["upload_status"] == "success":
            st.success("Ingest Raw File Successful, Calling Library...")
            library(dataset_name, version, s3_path, latest)
        if st.session_state["ingest"]["upload_status"] == "fail":
            st.error(st.session_state["ingest"]["error_message"])
            st.button("Restart", on_click=unlock)
        if st.session_state["ingest"]["library_status"] == "success":
            st.success("Ingest and Call Library Successful")
            st.button("Restart", on_click=unlock)
        if st.session_state["ingest"]["library_status"] == "fail":
            st.error(st.session_state["ingest"]["error_message"])
            st.session_state["ingest"]["s3_path"] = s3_path
            st.button("Call Library with File s3 Path", on_click=retry)
            st.button("Cancel Process", on_click=unlock)
