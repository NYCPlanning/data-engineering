from pathlib import Path

import typer

from dcpy.lifecycle.builds.artifacts import builds, drafts

app = typer.Typer()

# Add builds artifact commands
builds_app = typer.Typer()
app.add_typer(builds_app, name="builds")

# Add drafts artifact commands
drafts_app = typer.Typer()
app.add_typer(drafts_app, name="drafts")


@builds_app.command("upload")
def builds_upload(
    output_path: Path = typer.Argument(
        Path("output"), help="Path to local output folder"
    ),
    product: str = typer.Option(..., "-p", "--product", help="Name of data product"),
    build: str = typer.Option(None, "-b", "--build", help="Label of build"),
    acl: str = typer.Option(
        "public-read", "-a", "--acl", help="Access control level for uploaded files"
    ),
):
    """Upload build artifacts using the builds connector."""
    builds.upload(output_path, product, build, acl)


@builds_app.command("promote_to_draft")
def builds_promote_to_draft(
    product: str = typer.Option(..., "-p", "--product", help="Data product name"),
    build: str = typer.Option(..., "-b", "--build", help="Label of build"),
    draft_summary: str = typer.Option(
        "", "-ds", "--draft-summary", help="Draft description"
    ),
    acl: str = typer.Option(
        "public-read", "-a", "--acl", help="Access control level for uploaded files"
    ),
    download_metadata: bool = typer.Option(
        False, "-m", "--download-metadata", help="Download metadata after promotion"
    ),
):
    """Promote build to draft using the builds connector."""
    builds.promote_to_draft(
        product=product,
        build=build,
        acl=acl,
        draft_revision_summary=draft_summary,
        download_metadata=download_metadata,
    )


@drafts_app.command("publish")
def drafts_publish(
    product: str = typer.Option(..., "-p", "--product", help="Data product name"),
    version: str = typer.Option(
        ..., "-v", "--version", help="Data product release version"
    ),
    draft_revision_num: int = typer.Option(
        None,
        "-dn",
        "--draft-number",
        help="Draft revision number to publish. If blank, will use latest draft",
    ),
    acl: str = typer.Option(
        "public-read", "-a", "--acl", help="Access control level for uploaded files"
    ),
    latest: bool = typer.Option(
        False, "-l", "--latest", help="Publish to latest folder as well"
    ),
    is_patch: bool = typer.Option(
        False,
        "-ip",
        "--is-patch",
        help="Create a patched version if version already exists",
    ),
    download_metadata: bool = typer.Option(
        False,
        "-m",
        "--download-metadata",
        help="Download metadata from publish folder after publishing",
    ),
):
    """Publish draft to published using the drafts connector."""
    drafts.publish(
        product=product,
        version=version,
        draft_revision_num=draft_revision_num,
        acl=acl,
        latest=latest,
        is_patch=is_patch,
        download_metadata_to_file=download_metadata,
    )
