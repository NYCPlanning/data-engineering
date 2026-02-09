import json
from datetime import datetime

import pandas as pd
from src.shared.utils.publishing import unzip_csv

from dcpy.connectors.edm import publishing

PRODUCT = "db-pluto"


def get_data(product_key: publishing.ProductKey) -> dict[str, pd.DataFrame]:
    data = {}

    def read_pluto_csv(qaqc_type, **kwargs):
        return publishing.read_csv(
            product_key,
            f"qaqc/qaqc_{qaqc_type}.csv",
            true_values=["t"],
            false_values=["f"],
            **kwargs,
        )

    data["df_mismatch"] = read_pluto_csv("mismatch")
    data["df_null"] = read_pluto_csv("null")
    data["df_aggregate"] = read_pluto_csv("aggregate")
    data["df_expected"] = read_pluto_csv(
        "expected", converters={"expected": json.loads}
    )
    data["df_outlier"] = read_pluto_csv("outlier", converters={"outlier": json.loads})

    # only PLUTO 23v3+ versions are expected to have a bbl_diffs table
    try:
        data["df_bbl_diffs"] = read_pluto_csv("bbl_diffs")
    except FileNotFoundError:
        pass

    data = data | get_changes(product_key)

    data["source_data_versions"] = publishing.read_csv(
        product_key, "source_data_versions.csv"
    )

    data["version_text"] = data["source_data_versions"]

    # standarzie minor versions strings to be dot notation
    # NOTE this is a temporary approach until data-library is improved to use dot notation
    data_to_standardize = ["df_mismatch", "df_null"]
    for data_name in data_to_standardize:
        data[data_name].replace(
            to_replace={
                "23v1_1 - 23v1": "23v1.1 - 23v1",
                "23v1 - 22v3_1": "23v1 - 22v3.1",
            },
            inplace=True,
        )

    return data


def get_changes(product_key: publishing.ProductKey) -> dict[str, pd.DataFrame]:
    changes = {}
    pluto_changes_zip = publishing.get_zip(product_key, "pluto_changes.zip")
    changes["pluto_changes_applied"] = unzip_csv(
        csv_filename="pluto_changes_applied.csv",
        zipfile=pluto_changes_zip,
    )
    changes["pluto_changes_not_applied"] = unzip_csv(
        csv_filename="pluto_changes_not_applied.csv",
        zipfile=pluto_changes_zip,
    )

    return changes


def convert(dt):
    try:
        d = datetime.strptime(dt, "%Y/%m/%d")
        return d.strftime("%m/%d/%y")
    except BaseException:
        return dt
