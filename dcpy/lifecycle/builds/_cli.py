import typer

from dcpy.lifecycle.builds.artifacts.drafts import app as drafts_app
from dcpy.lifecycle.builds.build import app as build_app
from dcpy.lifecycle.builds.load import app as load_app
from dcpy.lifecycle.builds.plan import app as plan_app

app = typer.Typer()
app.add_typer(plan_app, name="plan")
app.add_typer(load_app, name="load")
app.add_typer(build_app, name="build")

# Create artifacts app with builds and drafts sub-apps
artifacts_app = typer.Typer()
artifacts_app.add_typer(drafts_app, name="drafts")
app.add_typer(artifacts_app, name="artifacts")
