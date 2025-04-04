import typer


from dcpy.lifecycle.builds.load import app as load_app
from dcpy.lifecycle.builds.publish import app as pub_app

app = typer.Typer()
app.add_typer(load_app, name="load")
app.add_typer(pub_app, name="publish")
