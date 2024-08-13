import typer

from dcpy.lifecycle.distribute import socrata
from dcpy.lifecycle.distribute import bytes

app = typer.Typer()

app.add_typer(socrata.socrata_app, name="socrata")
app.add_typer(bytes.app, name="bytes")
