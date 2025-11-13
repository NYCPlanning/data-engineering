from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle import product_metadata
from dcpy.utils.dates import very_fuzzy_date_compare
import pandas as pd


def sort_by_outdated_products(df):
    """
    Sort dataframe to show products with outdated datasets first.
    Products with any outdated datasets appear at the top.
    Also prioritizes products with open_data_versions over those with all blank versions.
    """
    # Create a summary of outdated status by product
    product_status = (
        df.groupby("product")["up_to_date"].agg(["all", "sum", "count"]).reset_index()
    )
    product_status["has_outdated"] = ~product_status["all"]
    product_status["outdated_count"] = product_status["count"] - product_status["sum"]

    # Add flag for products that have any open_data_versions (not all blank/missing)
    product_has_data = (
        df.groupby("product")["open_data_versions"]
        .apply(
            lambda x: x.apply(
                lambda v: bool(v and (v != [] if isinstance(v, list) else True))
            ).any()
        )
        .reset_index()
    )
    product_has_data.columns = ["product", "has_open_data"]
    product_status = product_status.merge(product_has_data, on="product")

    # Sort products:
    # 1. Those with outdated datasets first
    # 2. Those with open data first
    # 3. Then by number of outdated datasets
    product_order = product_status.sort_values(
        ["has_outdated", "has_open_data", "outdated_count"],
        ascending=[False, False, False],
    )["product"].tolist()

    # Reorder the dataframe based on product order
    df_sorted = df.reset_index()
    df_sorted["product_order"] = df_sorted["product"].map(
        {prod: i for i, prod in enumerate(product_order)}
    )
    df_sorted = df_sorted.sort_values(["product_order", "product", "dataset"]).drop(
        "product_order", axis=1
    )

    return df_sorted.set_index(["product", "dataset"])


def get_all_open_data_keys():
    """retrieve all product.dataset.destination_ids"""
    return product_metadata.load(version="dummy").query_product_dataset_destinations(
        destination_filter={"types": {"open_data"}},
    )


def get_open_data_versions(all_keys):
    open_data_con = connectors["open_data"]
    versions = {}
    for k in all_keys:
        try:
            versions[k] = open_data_con.get_latest_version(k)
        except Exception as e:
            versions[k] = e
    return versions


def get_bytes_versions(all_keys):
    bytes_keys = {k.rsplit(".", 1)[0] for k in all_keys}

    bytes_con = connectors["bytes"]
    versions_by_key = {}
    for k in bytes_keys:
        try:
            versions_by_key[k] = bytes_con.get_latest_version(k)
        except Exception as e:
            versions_by_key[k] = e
    return versions_by_key


def make_comparison_dataframe(bytes_versions, open_data_versions):
    rows = []
    for key in open_data_versions:
        product, dataset, destination_id = key.split(".")
        bytes_version = bytes_versions.get(f"{product}.{dataset}")
        open_data_vers = open_data_versions.get(key, [])

        # Determine if versions are up to date using fuzzy comparison
        up_to_date = False
        if bytes_version and open_data_vers:
            # Handle case where open_data_vers might be a list or single value
            if isinstance(open_data_vers, list) and open_data_vers:
                up_to_date = very_fuzzy_date_compare(bytes_version, open_data_vers[0])
            else:
                up_to_date = very_fuzzy_date_compare(bytes_version, open_data_vers)

        rows.append(
            {
                "product": product,
                "dataset": dataset,
                "destination_id": destination_id,
                "bytes_version": bytes_version,
                "open_data_versions": open_data_vers,
                "up_to_date": up_to_date,
            }
        )
    df = pd.DataFrame(rows).set_index(["product", "dataset"]).sort_index()

    # Add product-level up-to-date flag
    # A product is up-to-date if ALL its datasets are up-to-date
    product_status = df.groupby("product")["up_to_date"].all()
    df["product_up_to_date"] = df.index.get_level_values("product").map(product_status)

    return df


def run():
    all_keys = get_all_open_data_keys()
    open_data_versions = get_open_data_versions(all_keys)
    bytes_versions = get_bytes_versions(all_keys)
    df = make_comparison_dataframe(bytes_versions, open_data_versions)
    return sort_by_outdated_products(df)
