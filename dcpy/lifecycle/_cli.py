import typer

from dcpy.lifecycle.ingest import run
from dcpy.lifecycle.package._cli import app as package_app
from dcpy.lifecycle.distribute import _cli as distribute_cli


app = typer.Typer()
app.add_typer(package_app, name="package")
app.add_typer(distribute_cli.app, name="distribute")

# while there's only one ingest command, add it directly
app.command(name="ingest")(run._cli_wrapper_run)
