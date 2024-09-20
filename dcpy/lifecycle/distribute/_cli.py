import typer

from dcpy.lifecycle.distribute import socrata

app = typer.Typer()

app.add_typer(socrata.socrata_app, name="socrata")
