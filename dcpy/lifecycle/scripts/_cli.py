import typer

from .compare_build_tables import app as compare_build_tables_app
from .ingest_with_library_fallback import run as ingest_or_library_archive
from .package_and_distribute import app as package_dist_app
from .product_metadata import app as product_metadata_app
from .validate_ingest import app as ingest_validation_app

app = typer.Typer()

app.add_typer(product_metadata_app, name="product_metadata")
app.add_typer(ingest_validation_app, name="validate_ingest")
app.add_typer(package_dist_app, name="package_and_distribute")
app.add_typer(compare_build_tables_app, name="compare_build_tables")
app.command(name="ingest_or_library_archive")(ingest_or_library_archive)
