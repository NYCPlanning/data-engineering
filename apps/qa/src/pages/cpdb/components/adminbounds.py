import streamlit as st


def get_text(df, pre_df):
    output = list(
        set(pre_df.admin_boundary_type.unique()) - set(df.admin_boundary_type.unique())
    )

    if len(output) == 0:
        output = "No change"
    else:
        output = ",".join(output)

    return output


def adminbounds(data: dict):
    st.header(
        "Compare Administrative Boundary Values Between Previous and Latest Versions"
    )

    st.markdown(
        """
        Is there any differences in the adminstrative boundary values in previous vs. latest version? 
        The intended result is that the list is empty and all the admin boundaries are still present in the new output.
        Otherwise it might indicate that some of spatial join with admin boundaries have failed. 
        """
    )
    # admin bounds
    adminbounds_txt = get_text(data["cpdb_adminbounds"], data["pre_cpdb_adminbounds"])

    st.text(adminbounds_txt)
