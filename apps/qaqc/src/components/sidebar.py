import streamlit as st

from dcpy.utils import s3
from src.constants import BUCKET_NAME
from src.digital_ocean_utils import get_active_s3_folders


def branch_selectbox(repo, *, label="Select a branch", default=None, s3_folder=None):
    branches = get_active_s3_folders(repo=repo, s3_folder=s3_folder)
    if default:
        index = branches.index(default)
    else:
        index = 0  ## default arg for index in selectbox
    return st.sidebar.selectbox(
        label,
        branches,
        index=index,
    )


def output_selectbox(repo, branch, label="Select an export for comparison"):
    return st.sidebar.selectbox(
        label,
        s3.get_subfolders(BUCKET_NAME, f"{repo}/{branch}"),
        ##todo - all other than latest if same branch, or latest if other branch?
    )
