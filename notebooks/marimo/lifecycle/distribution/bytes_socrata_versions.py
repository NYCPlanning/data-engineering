import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Socrata <> Bytes version comparison tracker

    FYI, this might take a minute or two to run.
    """)
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    from dcpy.lifecycle.scripts import version_compare
    return mo, version_compare


@app.cell(hide_code=True)
def _(version_compare):
    versions = version_compare.run()
    return (versions,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Helpful links
    - Github action to distribute data: https://github.com/NYCPlanning/data-engineering/actions/workflows/distribute_socrata_from_bytes.yml
    - OpenData page to sign in and publish revisions: https://opendata.cityofnewyork.us/
    - Product Metadata repo: https://github.com/NYCPlanning/product-metadata
    """)
    return


@app.cell
def _(versions):
    versions.reset_index()
    return


@app.cell
def _():
    # all_open_data_keys = version_compare.get_all_open_data_keys()
    # all_open_data_keys
    return


if __name__ == "__main__":
    app.run()
