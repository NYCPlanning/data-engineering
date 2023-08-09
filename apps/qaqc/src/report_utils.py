import streamlit as st
from dcpy.connectors import github
from dcpy.utils import s3


@st.cache_data
def get_active_s3_folders(repo: str, bucket_name: str, s3_folder: str = None):
    default_branch = github.get_default_branch(repo=repo)
    all_branches = github.get_branches(repo=repo, branches_blacklist=[])
    all_folders = s3.get_subfolders(bucket_name, (s3_folder or repo))
    folders = sorted(list(set(all_folders).intersection(set(all_branches))))
    folders.remove(default_branch)
    folders = [default_branch] + folders
    return folders
