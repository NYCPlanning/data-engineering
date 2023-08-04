import pandas as pd
import streamlit as st

BUCKET_NAME = "edm-publishing"


def get_data(branch):
    rv = {}
    url = f"https://edm-publishing.nyc3.digitaloceanspaces.com/db-colp/{branch}/latest/output/qaqc"

    rv["modified_names"] = csv_from_DO(f"{url}/ipis_modified_names.csv")
    rv["records_by_agency"] = csv_from_DO(f"{url}/records_by_agency.csv")
    rv["records_by_usetype"] = csv_from_DO(f"{url}/records_by_usetype.csv")
    rv["records_by_agency_usetype"] = csv_from_DO(
        f"{url}/records_by_agency_usetype.csv"
    )

    rv["usetype_changes"] = csv_from_DO(f"{url}/usetype_changes.csv")

    rv["ipis_cd_errors"] = csv_from_DO(f"{url}/ipis_cd_errors.csv")
    rv["modifications_applied"] = csv_from_DO(f"{url}/modifications_applied.csv")
    rv["modifications_not_applied"] = csv_from_DO(
        f"{url}/modifications_not_applied.csv"
    )
    rv["geospatial_check"] = csv_from_DO(f"{url}/geospatial_check.csv")

    return rv


def csv_from_DO(url, kwargs={}):
    try:
        return pd.read_csv(url, **kwargs)
    except:
        st.warning(f"{url} not found")
