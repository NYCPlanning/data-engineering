import marimo

__generated_with = "0.17.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from dcpy.lifecycle.scripts import version_compare

    versions = version_compare.run()
    return (versions,)


@app.cell
def _(versions):
    versions
    return


if __name__ == "__main__":
    app.run()
