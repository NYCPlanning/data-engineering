# from pathlib import Path

# import typer

# from dcpy.connectors.edm.publishing import (
#     get_builds,
#     promote_to_draft,
# )
# from dcpy.lifecycle.connector_registry import connectors
# from dcpy.models.connectors.edm.publishing import BuildKey
# from dcpy.utils import s3

# app = typer.Typer(add_completion=False)


# @app.command("upload")
# def upload_build_cli(
#     product: str = typer.Argument(help="product name"),
#     build_path: str = typer.Option(".", help="path to build output folder"),
#     acl: str = typer.Option(default="public-read", help="s3 acl"),
#     build_note: str = typer.Option("", help="build note (optional)"),
#     max_files: int = typer.Option(200, help="maximum number of files to upload"),
# ):
#     """Upload build output to publishing bucket"""
#     connector = connectors["edm.publishing.builds"]
#     result = connector.push(
#         key=product,
#         build_path=Path(build_path),
#         connector_args={
#             "acl": acl,
#             "build_note": build_note,
#             "max_files": max_files,
#         },
#     )
#     typer.echo(f"Uploaded build: {result}")


# @app.command("pull")
# def pull_build_cli(
#     product: str = typer.Argument(help="product name"),
#     build_name: str = typer.Argument(help="build name to pull"),
#     download_path: str = typer.Option(".", help="local path to download build to"),
# ):
#     """Pull build output from publishing bucket"""
#     builds_connector = connectors["edm.publishing.builds"]

#     build_key = BuildKey(product, build_name)

#     result_path = builds_connector.pull(
#         key=build_key,
#         destination_path=Path(download_path),
#     )

#     typer.echo(f"Pulled build to: {result_path}")


# @app.command("list")
# def list_builds_cli(
#     product: str = typer.Argument(help="product name"),
# ):
#     """List all builds for a product"""
#     builds_connector = connectors["edm.publishing.builds"]
#     builds = get_builds(product, storage=builds_connector.storage)

#     if builds:
#         typer.echo(f"Builds for {product}:")
#         for build in builds:
#             typer.echo(f"  {build}")
#     else:
#         typer.echo(f"No builds found for {product}")


# @app.command("promote_to_draft")
# def promote_to_draft_cli(
#     product: str = typer.Argument(help="product name"),
#     build_name: str = typer.Argument(help="build name to promote"),
#     acl: str = typer.Option(default="public-read", help="s3 acl"),
#     keep_build: bool = typer.Option(True, help="keep build folder after promotion"),
#     draft_revision_summary: str = typer.Option("", help="draft revision summary"),
#     download_metadata: bool = typer.Option(
#         False, help="download metadata after promotion"
#     ),
# ):
#     """Promote a build to draft"""
#     builds_connector = connectors["edm.publishing.builds"]

#     build_key = BuildKey(product, build_name)

#     # Use the promote function with storage from the builds connector
#     draft_key = promote_to_draft(
#         build_key,
#         acl=s3.string_as_acl(acl),
#         keep_build=keep_build,
#         draft_revision_summary=draft_revision_summary,
#         download_metadata=download_metadata,
#         storage=builds_connector.storage,
#     )

#     typer.echo(f"Promoted {build_key} to draft: {draft_key}")
