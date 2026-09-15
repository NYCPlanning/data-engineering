import typer

from .excel import app as excel_app
from .s3 import app as s3_app

app = typer.Typer()
app.add_typer(s3_app, name="s3")
app.add_typer(excel_app, name="excel")
