"""Copy PFF build output into the `edm-publishing` bucket.

PFF can't use `dcpy`'s DraftKey/PublishKey: the downstream app builds its URL as
`$BASE_URL/$datasource/$vintage/$version/$datasource.csv`, one path segment each, so the
release version and draft revision have to be fused into a single `2026_3` segment.
Hand-assembling that path is how `publish/acs/2026/2024` ended up with its segments
reversed, so the order is defined once here rather than at each call site.
"""

import re
from pathlib import Path

import typer

from dcpy.configuration import PUBLISHING_BUCKET
from dcpy.utils import s3

from .pipelines import OUTPUT_FOLDER

PRODUCT = "db-factfinder"
DATASOURCES = ("acs", "decennial")
DRAFT_VERSION = re.compile(r"^(?P<version>\d{4})_(?P<revision>\d+)$")


def _bucket() -> str:
    assert PUBLISHING_BUCKET, "'PUBLISHING_BUCKET' must be set"
    return PUBLISHING_BUCKET


def s3_folder(stage: str, datasource: str, vintage: str, version: str) -> str:
    """`<product>/<stage>/<datasource>/<vintage>/<version>` — the only place this order is defined."""
    assert datasource in DATASOURCES, f"datasource must be one of {DATASOURCES}"
    assert re.fullmatch(r"\d{4}", vintage), f"vintage must be a year, got '{vintage}'"
    # a release can't predate the data it describes; catches vintage/version swapped
    assert int(version[:4]) >= int(vintage), (
        f"version '{version}' is older than vintage '{vintage}' — arguments swapped?"
    )
    return f"{PRODUCT}/{stage}/{datasource}/{vintage}/{version}"


app = typer.Typer(add_completion=False)


@app.command()
def draft(datasource: str, vintage: str, version: str) -> None:
    """Upload local build output to `draft/`, e.g. `acs 2024 2026_3`."""
    assert DRAFT_VERSION.fullmatch(version), (
        f"draft version must look like '2026_3', got '{version}'"
    )
    target = s3_folder("draft", datasource, vintage, version)
    s3.upload_folder(
        _bucket(),
        OUTPUT_FOLDER / datasource / vintage,
        Path(target),
        acl="public-read",
        contents_only=True,
    )


@app.command()
def publish(datasource: str, vintage: str, draft_version: str) -> None:
    """Promote a QA'd draft to `publish/`, dropping the revision suffix."""
    match = DRAFT_VERSION.fullmatch(draft_version)
    assert match, f"draft version must look like '2026_3', got '{draft_version}'"
    source = s3_folder("draft", datasource, vintage, draft_version)
    target = s3_folder("publish", datasource, vintage, match["version"])
    s3.copy_folder(_bucket(), source, target, acl="public-read")


if __name__ == "__main__":
    app()
