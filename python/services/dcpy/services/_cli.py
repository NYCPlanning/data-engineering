import typer

from .catalog import _cli as catalog_cli

app = typer.Typer()
app.add_typer(catalog_cli.app, name="catalog")
