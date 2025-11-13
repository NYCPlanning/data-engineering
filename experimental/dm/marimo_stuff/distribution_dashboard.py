import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo
    import pandas as pd

    from dcpy.connectors.edm.bytes import BytesConnector
    from dcpy.connectors.edm.open_data_nyc import OpenDataConnector
    from dcpy.lifecycle import product_metadata
    from dcpy.lifecycle.scripts import version_compare


@app.cell
def _():
    versions = version_compare.run()
    return (versions,)


@app.cell
def _(versions):
    versions
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Distribution Dashboard

    This notebook is for comparing the versions of datasets on Bytes and Open Data to inform distribution of data updates.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    /// details | Environemnt variable details
        type: warn

    Some environemnt variables must be set before running this notebook:
    - `PRODUCT_METADATA_REPO_PATH`
    - `SOCRATA_USER`
    - `SOCRATA_PASSWORD`
    """)
    return


@app.cell
def _():
    # all_latest_bytes_versions = BytesConnector().fetch_all_latest_versions_df()
    return


@app.cell
def _():
    # all_latest_bytes_versions
    return


@app.cell
def _():
    data_engineering_datasets = [
        {
            "base_key": "zap.bbls",
            "open_data_destination_id": "socrata",
        },
        {
            "base_key": "zap.projects",
            "open_data_destination_id": "socrata",
        },
        {
            "base_key": "ztl.ztl",
            "open_data_destination_id": "socrata",
        },
    ]
    return (data_engineering_datasets,)


@app.function
def open_data_url(key: str):
    product, dataset, destination_id = key.split(".")
    metadata = product_metadata.load(version="dummy")
    four_four = (
        metadata.product(product)
        .dataset(dataset)
        .get_destination(destination_id)
        .custom.get("four_four")
    )
    return f"https://data.cityofnewyork.us/d/{four_four}"


@app.cell
def _(data_engineering_datasets):
    def check_versions(datasets: list[dict]) -> pd.DataFrame:
        for dataset in data_engineering_datasets:
            open_data_key = ".".join(
                [dataset["base_key"], dataset["open_data_destination_id"]]
            )
            url = open_data_url(open_data_key)
            print(dataset["base_key"])
            print(BytesConnector().get_latest_version(dataset["base_key"]))
            print(OpenDataConnector().list_versions(open_data_key))
            print(url)
            print("-----")
    return (check_versions,)


@app.cell
def _(check_versions, data_engineering_datasets):
    check_versions(data_engineering_datasets)
    return


@app.cell
def _():
    colp_open_data_version = OpenDataConnector().list_versions(
        key="colp.colp.socrata"
    )
    colp_open_data_version
    return


if __name__ == "__main__":
    app.run()
