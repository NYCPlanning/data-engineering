import typer
from .package_and_distribute import app as package_dist_app
from .product_metadata import app as product_metadata_app

app = typer.Typer()

app.add_typer(package_dist_app, name="package_and_dist")
app.add_typer(product_metadata_app, name="product_metadata")
