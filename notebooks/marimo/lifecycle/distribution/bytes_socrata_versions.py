import marimo

__generated_with = "0.19.7"
app = marimo.App(width="full")

with app.setup:
    import marimo as mo

    from dcpy.lifecycle.scripts import version_compare


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Socrata <> Bytes version comparison tracker
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Helpful links
    - Github action to distribute from Bytes to Open Data: https://github.com/NYCPlanning/data-engineering/actions/workflows/distribute_socrata_from_bytes.yml
    - Open Data page to sign in and publish revisions: https://opendata.cityofnewyork.us/
    - Product Metadata repo: https://github.com/NYCPlanning/product-metadata
    """)
    return


@app.cell
def _(versions):
    mo.ui.table(versions.reset_index(), page_size=25, selection=None)
    return


@app.cell(hide_code=True)
def _():
    with mo.status.spinner(title="Fetching versions from Bytes and Open Data ..."):
        versions = version_compare.run()
    return (versions,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
