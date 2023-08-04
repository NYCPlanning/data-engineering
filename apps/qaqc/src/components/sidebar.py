import streamlit as st

from src.report_utils import get_active_s3_folders
from src.digital_ocean_utils import DigitalOceanClient


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
        DigitalOceanClient(
            bucket_name=bucket,
            repo_name=f"{repo}/{branch}",
        ).get_all_folder_names_in_repo_folder(
            index=2
        ),  ##todo - all other than latest if same branch, or latest if other branch?
    )
