import typer

from .validate import _validate
from .esri import app as esri_app

app = typer.Typer()
app.command(name="validate")(_validate)
app.add_typer(esri_app, name="esri")
