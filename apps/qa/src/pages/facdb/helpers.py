import pandas as pd

from dcpy.connectors.edm import publishing

PRODUCT = "db-facilities"
REPO_NAME = "data-engineering"


def get_latest_data(
    product_key: publishing.ProductKey,
) -> tuple[dict[str, dict[str, object]], pd.DataFrame, pd.DataFrame]:
    def read_csv(csv):
        return publishing.read_csv(product_key, f"{csv}.csv")

    qc_diff = read_csv("qc_diff")
    qc_captype = read_csv("qc_captype")
    qc_classification = read_csv("qc_classification")
    qc_mapped = read_csv("qc_mapped")
    qc_operator = read_csv("qc_operator")
    qc_oversight = read_csv("qc_oversight")

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
