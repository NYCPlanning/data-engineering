import typer

from dcpy.lifecycle.data_loader import app as data_loader_app
from dcpy.lifecycle.ingest import _cli_wrapper_run as run_ingest
from dcpy.lifecycle.builds._cli import app as builds_app
from dcpy.lifecycle.package._cli import app as package_app
from dcpy.lifecycle.distribute import _cli as distribute_cli
from dcpy.lifecycle.scripts import _cli as scripts_cli
from dcpy.lifecycle._connectors_cli import app as connectors_app

app = typer.Typer()
app.add_typer(builds_app, name="builds")
app.add_typer(package_app, name="package")
app.add_typer(distribute_cli.app, name="distribute")
app.add_typer(scripts_cli.app, name="scripts")
app.add_typer(data_loader_app, name="data_loader")
app.add_typer(connectors_app, name="connectors")

# while there's only one ingest command, add it directly
app.command(name="ingest")(run_ingest)
