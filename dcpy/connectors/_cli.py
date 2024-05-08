import typer

from .edm.packaging import app as package_app

edm_app = typer.Typer()
edm_app.add_typer(package_app, name="package")


app = typer.Typer()
app.add_typer(edm_app, name="edm")
