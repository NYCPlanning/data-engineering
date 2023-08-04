import streamlit as st

from dcpy.connectors import s3
from src.report_utils import get_active_s3_folders


def branch_selectbox(
    repo, bucket, label="Select a branch", default=None, s3_folder=None
):
    branches = get_active_s3_folders(repo=repo, bucket_name=bucket, s3_folder=s3_folder)
    if default:
        index = branches.index(default)
    else:
        index = 0  ## default arg for index in selectbox
    return st.sidebar.selectbox(
        label,
        branches,
        index=index,
    )


def output_selectbox(repo, bucket, branch, label="Select an export for comparison"):
    return st.sidebar.selectbox(
        label,
        s3.get_subfolders(bucket, f"{repo}/{branch}"),
        ##todo - all other than latest if same branch, or latest if other branch?
    )
