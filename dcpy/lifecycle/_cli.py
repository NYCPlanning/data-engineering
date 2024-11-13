import typer

from dcpy.lifecycle.ingest import run as ingest
from dcpy.lifecycle.package._cli import app as package_app
from dcpy.lifecycle.distribute import _cli as distribute_cli
from dcpy.lifecycle.scripts import _cli as scripts_cli

app = typer.Typer()
app.add_typer(package_app, name="package")
app.add_typer(distribute_cli.app, name="distribute")
app.add_typer(scripts_cli.app, name="scripts")

# while there's only one ingest command, add it directly
app.command(name="ingest")(ingest._cli_wrapper_run)
