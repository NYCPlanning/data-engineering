import pandas as pd
from typing import Union
from dcpy.connectors.edm import publishing

BUCKET_NAME = "edm-publishing"
PRODUCT_NAME = "db-facilities"
REPO_NAME = "data-engineering"


def get_latest_data(
    branch,
) -> tuple[dict[str, dict[str, Union[pd.DataFrame, str]]], pd.DataFrame, pd.DataFrame]:
    url = publishing.get_dataset_branch_path(
        dataset=PRODUCT_NAME,
        branch=branch,
        version="latest",
    )

    qc_diff = pd.read_csv(f"{url}/qc_diff.csv")
    qc_captype = pd.read_csv(f"{url}/qc_captype.csv")
    qc_classification = pd.read_csv(f"{url}/qc_classification.csv")
    qc_mapped = pd.read_csv(f"{url}/qc_mapped.csv")
    qc_operator = pd.read_csv(f"{url}/qc_operator.csv")
    qc_oversight = pd.read_csv(f"{url}/qc_oversight.csv")

    qc_tables = {
        "Facility subgroup classification": {
            "dataframe": qc_classification,
            "type": "dataframe",
        },
        "Operator": {
            "dataframe": qc_operator,
            "type": "dataframe",
        },
        "Oversight": {
            "dataframe": qc_oversight,
            "type": "dataframe",
        },
        "Capacity Types": {
            "dataframe": qc_captype,
            "type": "table",
        },
    }
    return qc_tables, qc_diff, qc_mapped
