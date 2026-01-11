# from typing import cast

import typer

# from dcpy.connectors.edm.drafts import get_draft_version_revisions, get_draft_versions
# from dcpy.connectors.edm.drafts import publish as publish_function
# from dcpy.connectors.edm.publishing import (
#     validate_or_patch_version,
# )
# from dcpy.lifecycle.connector_registry import connectors
# from dcpy.models.connectors.edm.publishing import DraftKey
# from dcpy.utils import s3

app = typer.Typer(add_completion=False)


# @app.command("publish")
# def publish_cli(
#     product: str = typer.Argument(help="product name"),
#     version: str = typer.Argument(help="version to publish from drafts"),
#     draft_revision_label: str = typer.Argument(help="draft revision label to publish"),
#     acl: str = typer.Option(default="public-read", help="s3 acl"),
#     is_patch: bool = typer.Option(False, help="whether this is a patch version"),
#     latest: bool = typer.Option(False, help="update 'latest' folder"),
#     download_metadata: bool = typer.Option(
#         False, help="download metadata after publishing"
#     ),
# ):
#     """Publish a draft version"""
#     drafts_connector = connectors["edm.publishing.drafts"]

#     draft_key = DraftKey(product, version, draft_revision_label)

#     # Use the publish function with the storage from the drafts connector
#     publish_key = publish_function(
#         draft_key,
#         acl=s3.string_as_acl(acl),
#         latest=latest,
#         is_patch=is_patch,
#         download_metadata=download_metadata,
#         storage=drafts_connector.storage,
#     )

#     typer.echo(f"Published {draft_key} as {publish_key}")


# @app.command("validate_or_patch_version")
# def validate_or_patch_version_cli(
#     product: str = typer.Argument(help="product name"),
#     version: str = typer.Argument(help="version to validate or patch"),
#     is_patch: bool = typer.Option(False, help="whether this is a patch version"),
# ):
#     """Validate or generate a patch version"""
#     published_connector = connectors["edm.publishing.published"]

#     new_version = validate_or_patch_version(
#         product=product,
#         version=version,
#         is_patch=is_patch,
#         storage=published_connector.storage,
#     )

#     typer.echo(f"Version: {new_version}")


# @app.command("list")
# def list_drafts_cli(
#     product: str = typer.Argument(help="product name"),
# ):
#     """List all draft versions for a product"""
#     drafts = get_draft_versions(product)

#     if drafts:
#         typer.echo(f"Draft versions for {product}:")
#         for draft in drafts:
#             typer.echo(f"  {draft}")
#     else:
#         typer.echo(f"No draft versions found for {product}")


# @app.command("revisions")
# def list_draft_versions_cli(
#     product: str = typer.Argument(help="product name"),
#     version: str = typer.Argument(help="version"),
# ):
#     """List all draft version revisions for a product"""
#     draft_revisions = get_draft_version_revisions(product, version)

#     if draft_revisions:
#         typer.echo(f"Draft version revisions for {product}:")
#         for draft_revision in draft_revisions:
#             typer.echo(f"  {draft_revision}")
#     else:
#         typer.echo(f"No draft version revisions found for {product}")
