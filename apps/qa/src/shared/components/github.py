import pytz
import streamlit as st

from dcpy.utils.git import github


def dispatch_workflow_button(
    repo, workflow_name, key, label="Run", disabled=False, run_after=None, **inputs
):
    def on_click():
        github.dispatch_workflow(repo, workflow_name, **inputs)
        if run_after is not None:
            run_after()

    return st.button(label, key=key, on_click=on_click, disabled=disabled)


@st.cache_data(ttl=300)
def list_dispatchable_branches(repo: str) -> list[str]:
    """Branch names for a dispatch dialog's dropdown, newest-noise filtered out.

    Dependabot opens (and leaves behind) a branch per dependency bump, which would
    otherwise dominate the list without ever being a real dispatch target.
    """
    branches = [b for b in github.get_branches(repo) if not b.startswith("dependabot/")]
    return sorted(branches)


def find_run(repo: str, workflow_name: str, run_name: str) -> github.WorkflowRun | None:
    """Find the most recent run of a workflow whose (dispatch-templated) run-name matches.

    Relies on the workflow's own `run-name:` embedding its dispatch inputs (e.g.
    "Ingest Single Dataset: {dataset} {version}"), so a specific dispatch can be found again.
    """
    for run in github.get_workflow_runs(repo, workflow_name, total_items=50):
        if run.name == run_name:
            return run
    return None


def status_details(workflow_run: github.WorkflowRun) -> None:
    timestamp = workflow_run.timestamp.astimezone(pytz.timezone("US/Eastern")).strftime(
        "%Y-%m-%d %H:%M"
    )

    def format(status: str) -> str:
        return f"{status}  \n[{timestamp}]({workflow_run.url})"

    if workflow_run.is_running:
        st.warning(format(workflow_run.status.capitalize().replace("_", " ")))
        st.spinner()
    elif workflow_run.status == "completed":
        if workflow_run.conclusion == "success":
            st.success(format("Success"))
        elif workflow_run.conclusion == "cancelled":
            st.info(format("Cancelled"))
        elif workflow_run.conclusion == "failure":
            st.error(format("Failed"))
        else:
            st.write(workflow_run.conclusion)
