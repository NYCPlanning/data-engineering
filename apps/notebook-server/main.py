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

        # TODO: we can worry about navigation, and auto-discovery of notebooks later.
        - [PLUTO MIH Area Analysis](/notebooks/marimo/products/pluto/far)
        """
    )
    return


@app.cell
def __():
    import marimo as mo

    return (mo,)


if __name__ == "__main__":
    app.run()
