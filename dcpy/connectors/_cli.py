import typer

from .edm.packaging import app as package_app
from .socrata import metadata

edm_app = typer.Typer()
edm_app.add_typer(package_app, name="package")

socrata_app = typer.Typer()
socrata_app.add_typer(metadata.app, name="metadata")

app = typer.Typer()
app.add_typer(edm_app, name="edm")
app.add_typer(socrata_app, name="socrata")
