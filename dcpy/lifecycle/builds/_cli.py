import typer


from dcpy.lifecycle.builds.load import app as load_app
from dcpy.lifecycle.builds.plan import app as plan_app
from dcpy.lifecycle.builds.build import app as build_app

app = typer.Typer()
app.add_typer(plan_app, name="plan")
app.add_typer(build_app, name="build")
app.add_typer(load_app, name="load")
