import streamlit as st
from src.digital_ocean_utils import DigitalOceanClient
from src.github import get_default_branch, get_branches


@st.cache_data
def get_active_s3_folders(repo:str, bucket_name:str, s3_folder:str=None):
    default_branch = get_default_branch(repo=repo)
    all_branches = get_branches(repo=repo, branches_blacklist=[])
    all_folders = DigitalOceanClient(
        bucket_name=bucket_name, repo_name=(s3_folder or repo),
    ).get_all_folder_names_in_repo_folder()

    folders = sorted(list(set(all_folders).intersection(set(all_branches))))
    folders.remove(default_branch)
    folders = [default_branch] + folders
    return folders