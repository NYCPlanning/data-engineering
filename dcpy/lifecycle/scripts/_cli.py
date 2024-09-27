import typer
from .package_and_distribute import app as package_dist_app

app = typer.Typer()

app.add_typer(package_dist_app, name="package_and_dist")
