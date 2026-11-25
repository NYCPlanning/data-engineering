import typer

from .compare_build_exports import app as compare_build_exports_app
from .compare_build_tables import app as compare_build_tables_app
from .distribute_pluto_to_dof import app as distribute_pluto_to_dof_app
from .ingest import run as ingest
from .package_and_distribute import app as package_dist_app

app = typer.Typer()

app.add_typer(package_dist_app, name="package_and_distribute")
app.add_typer(compare_build_tables_app, name="compare_build_tables")
app.add_typer(compare_build_exports_app, name="compare_build_exports")
app.add_typer(distribute_pluto_to_dof_app, name="distribute_pluto_to_dof")
app.command(name="ingest")(ingest)
