import streamlit as st

from dcpy.utils import s3

from .helpers import compare_columns, get_active_s3_folders


def column_comparison_table(old_data, new_data):
    dropped, added, union, paired = compare_columns(old_data, new_data, True)
    nrows_mismatch = max(len(dropped), len(added))

    with st.expander(f"{len(union)} matching columns"):
        for col in union:
            st.info(col)

    with st.expander(f"{len(paired)} columns with updated year in header"):
        col1, col2 = st.columns(2)
        col1.write("Old")
        col2.write("Updated")

        for old_column, matched_columns in paired:
            with col1:
                st.warning(old_column)
            with col2:
                st.warning(", ".join(matched_columns))

    with st.expander(f"{len(dropped)} columns dropped, {len(added)} columns added"):
        col1, col2 = st.columns(2)
        col1.write("Dropped")
        col2.write("Added")

        for i in range(nrows_mismatch):
            with col1:
                if i < len(dropped):
                    st.error(dropped[i])
            with col2:
                if i < len(added):
                    st.success(added[i])


## TODO when edde outputs are refactored, remove references to s3 and use edm.publishing instead
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
        s3.get_subfolders("edm-publishing", f"{repo}/{branch}"),
        ##todo - all other than latest if same branch, or latest if other branch?
    )
