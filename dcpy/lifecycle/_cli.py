import typer

from dcpy.lifecycle.package import validate
from dcpy.lifecycle.distribute import socrata

app = typer.Typer()
app.add_typer(validate.app, name="package")
app.add_typer(socrata.distribute_app, name="distribute")
