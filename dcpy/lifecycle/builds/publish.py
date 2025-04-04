from pathlib import Path
import typer

from dcpy.lifecycle.connector_registry import connectors
from dcpy.lifecycle.config import CONF
from dcpy.models.lifecycle.builds import BuildMetadata

app = typer.Typer(add_completion=False)

pub_conf = CONF["stages"]["builds"]["stages"]["publish"]


@app.command("draft")
def draft(path: Path, build_metadata_path: Path = Path("build_metadata.json")):
    """Push a build to edm.publishing.drafts"""
    draft_connector = pub_conf["draft"]["connector"]
    connector = connectors[draft_connector]
    build_metadata = BuildMetadata.from_file(path / build_metadata_path)

    product = build_metadata.recipe.product
    version = build_metadata.version

    typer.echo(f"Publishing Draft for {product} with version={version}")

    connector.push(
        path=Path("?"),  # HUH...
        key=product,
        version=version,
        push_conf=build_metadata.model_dump(),
    )
