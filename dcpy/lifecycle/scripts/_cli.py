import typer
from .package_and_distribute import package_and_distribute_cli
from .product_metadata import app as product_metadata_app
from .ingest_with_library_fallback import run as ingest_or_library_archive
from .validate_ingest import app as ingest_validation_app

app = typer.Typer()

app.add_typer(product_metadata_app, name="product_metadata")
app.add_typer(ingest_validation_app, name="validate_ingest")
app.command(name="package_and_distribute")(package_and_distribute_cli)
app.command(name="ingest_or_library_archive")(ingest_or_library_archive)
