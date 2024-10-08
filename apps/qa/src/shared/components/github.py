import streamlit as st
from dcpy.connectors.github import dispatch_workflow


def dispatch_workflow_button(
    repo, workflow_name, key, label="Run", disabled=False, run_after=None, **inputs
):
    def on_click():
        dispatch_workflow(repo, workflow_name, **inputs)
        if run_after is not None:
            run_after()

    return st.button(label, key=key, on_click=on_click, disabled=disabled)
