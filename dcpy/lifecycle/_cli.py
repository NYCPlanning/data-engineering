import typer

from dcpy.lifecycle.ingest import run
from dcpy.lifecycle.package import validate
from dcpy.lifecycle.distribute import socrata

app = typer.Typer()
app.add_typer(validate.app, name="package")
app.add_typer(socrata.distribute_app, name="distribute")
# while there's only one ingest command, add it directly
app.command(name="ingest")(run._cli_wrapper_run)
