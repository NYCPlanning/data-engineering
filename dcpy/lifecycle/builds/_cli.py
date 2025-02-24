import typer


from dcpy.lifecycle.builds.load import app as load_app

app = typer.Typer()
app.add_typer(load_app, name="load")
