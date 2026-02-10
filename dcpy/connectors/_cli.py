import typer

from .edm.packaging import package_app as package_app
from .esri import arcgis_feature_service
from .socrata import metadata

edm_app = typer.Typer()
edm_app.add_typer(package_app, name="packaging")

socrata_app = typer.Typer()
socrata_app.add_typer(metadata.app, name="metadata")

app = typer.Typer()
app.add_typer(edm_app, name="edm")
app.add_typer(socrata_app, name="socrata")
app.add_typer(arcgis_feature_service.app, name="esri")
