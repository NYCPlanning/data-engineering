import typer

from .esri import arcgis_feature_service
from .socrata import metadata

socrata_app = typer.Typer()
socrata_app.add_typer(metadata.app, name="metadata")

app = typer.Typer()
app.add_typer(socrata_app, name="socrata")
app.add_typer(arcgis_feature_service.app, name="esri")
