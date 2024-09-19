import typer
import dcpy.lifecycle._cli as lifecycle
import dcpy.connectors._cli as connectors
import dcpy.utils._cli as utils

# This is a separate module because it will eventually import most of dcpy
# as we expand functionality. There maybe be cases where we don't want to do that,
# e.g. if we just want to export one submodule (e.g. utils)

if __name__ == "__main__":
    app = typer.Typer()

    app.add_typer(lifecycle.app, name="lifecycle")
    app.add_typer(connectors.app, name="connectors")
    app.add_typer(utils.app, name="utils")
    app()
