import marimo

__generated_with = "0.18.3"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Socrata <> Bytes version comparison tracker

    FYI, this might take a minute or two to run.
    """)
    return


@app.cell
def _():
    from dcpy.lifecycle.scripts import version_compare

    versions = version_compare.run()
    return (versions,)


@app.cell
def _(versions):
    versions
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
