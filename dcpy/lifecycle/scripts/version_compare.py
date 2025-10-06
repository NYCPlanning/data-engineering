from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle import product_metadata
import pandas as pd


def get_all_open_data_keys():
    """retrieve all product.dataset.destination_ids"""
    return product_metadata.load(version="dummy").query_product_dataset_destinations(
        # product_ids={"lion"},
        # dataset_ids={"atomic_polygons", "lion"},
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
        rows.append(
            {
                "product": product,
                "dataset": dataset,
                "destination_id": destination_id,
                "bytes_version": bytes_version,
                "open_data_versions": open_data_vers,
            }
        )
    df = pd.DataFrame(rows).set_index(["product", "dataset"]).sort_index()
    return df


def run():
    all_keys = get_all_open_data_keys()
    open_data_versions = get_open_data_versions(all_keys)
    bytes_versions = get_bytes_versions(all_keys)
    return make_comparison_dataframe(bytes_versions, open_data_versions)
