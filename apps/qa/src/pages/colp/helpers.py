import pandas as pd
import streamlit as st

from dcpy.connectors.edm import publishing

PRODUCT = "db-colp"


def get_data(product_key: publishing.ProductKey) -> dict[str, pd.DataFrame]:
    rv = {}

    def csv_from_DO(file):
        try:
            return publishing.read_csv(product_key, "qaqc/" + file)
        except FileNotFoundError:
            st.warning(f"{file} not found")

    rv["modified_names"] = csv_from_DO("ipis_modified_names.csv")
    rv["records_by_agency"] = csv_from_DO("records_by_agency.csv")
    rv["records_by_usetype"] = csv_from_DO("records_by_usetype.csv")
    rv["records_by_agency_usetype"] = csv_from_DO("records_by_agency_usetype.csv")

    rv["usetype_changes"] = csv_from_DO("usetype_changes.csv")

    rv["ipis_cd_errors"] = csv_from_DO("ipis_cd_errors.csv")
    rv["modifications_applied"] = csv_from_DO("modifications_applied.csv")
    rv["modifications_not_applied"] = csv_from_DO("modifications_not_applied.csv")
    rv["geospatial_check"] = csv_from_DO("geospatial_check.csv")

    return rv
