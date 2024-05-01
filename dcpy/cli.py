import typer
import dcpy.lifecycle.cli as lifecycle

# This is a separate module because it will eventually import most of dcpy
# as we expand functionality. There maybe be cases where we don't want to do that,
# e.g. if we just want to export one submodule (e.g. utils)

if __name__ == "__main__":
    app = typer.Typer()  # add_completion=False)

    app.add_typer(lifecycle.app, name="lifecycle")
    app()
