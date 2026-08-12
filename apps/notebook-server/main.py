import marimo

__generated_with = "0.8.0"
app = marimo.App(app_title="Data Engineering Notebooks")


@app.cell
def __(mo):
    mo.md(
        r"""
        # Data Engineering Notebooks

        Welcome to the NYC DCP Data Engineering notebook server.

        ## Available Notebooks

        None at the moment. The Bytes/Open Data version tracker moved to the QA app's
        [Data Distribution](/qaqc/?page=Data%20Distribution) page, which also dispatches
        the distribution workflow.
        """
    )
    return


@app.cell
def __():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
