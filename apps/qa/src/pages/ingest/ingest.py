def ingest():
    import streamlit as st
    from .helpers import archive_raw_data
    from pathlib import Path
    import time

    st.title("Ingest Dataset")

    if "running" not in st.session_state:
        st.session_state.running = False

    dataset_name = st.text_input("Dataset Name", disabled=st.session_state.running)
    version = st.text_input("Version", disabled=st.session_state.running)
    uploaded_file = st.file_uploader("Choose a file", disabled=st.session_state.running)
    s3_path = Path("inbox") / dataset_name / version

    disabled = st.session_state.get("running", False)
    ingest_button_pressed = st.button("Ingest", disabled=st.session_state.running)
    if ingest_button_pressed == True:
        if dataset_name and version and uploaded_file:
            st.session_state.running = True
            st.rerun()
        else:
            st.warning("Please input all fields.")

    if st.session_state.running == True:
        with st.spinner("Ingesting"):
            time.sleep(5)
            file_name = uploaded_file.name
            try:
                file_path = archive_raw_data(
                    dataset_name, version, uploaded_file, file_name
                )
            except Exception as e:
                st.error("Ingestion Failed: {e}")
                time.sleep(5)
                st.session_state.running = False
                st.rerun()
        st.session_state.running = False
        st.success("Ingest Successful")
        time.sleep(5)
        st.rerun()
