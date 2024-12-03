import typer

from .validate import _validate
from .esri import app as esri_app

from .assemble import app as assemble_app
from .xlsx_writer import app as xlsx_writer_app
from .shapefiles import app as shapefile_app

app = typer.Typer()
app.command(name="validate")(_validate)
app.add_typer(esri_app, name="esri")
app.add_typer(assemble_app, name="assemble")
app.add_typer(xlsx_writer_app, name="oti")
app.add_typer(shapefile_app, name="shapefile")
