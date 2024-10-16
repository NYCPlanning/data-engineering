import typer

from .validate import _validate
from .esri import app as esri_app

from .assemble import app as assemble_app
from .oti_xlsx import app as oti_xlsx_app
from .shapefiles import app as shapefile_app

app = typer.Typer()
app.command(name="validate")(_validate)
app.add_typer(esri_app, name="esri")
app.add_typer(assemble_app, name="assemble")
app.add_typer(oti_xlsx_app, name="oti")
app.add_typer(shapefile_app, name="shapefile")
