import typer

from .s3 import app as s3_app

app = typer.Typer()
app.add_typer(s3_app, name="s3")
