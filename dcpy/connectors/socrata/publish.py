"""DCP Socrata Connector to bridge the gaps between various Socrata APIs/SDKs.

The publishing API documentation is here:
https://dev.socrata.com/docs/other/publishing.html#?route=overview

Additionally, when it's mysterious about which endpoint to hit, or what the
arguments should be, opening the web interface and watching the requests is
quite informative.
"""

from __future__ import annotations
from dataclasses import dataclass
import json
import os
from pathlib import Path
import requests
from socrata.authorization import Authorization
from socrata import Socrata as SocrataPy
import typer
from typing import TypedDict, Literal

from .metadata import datasets

from dcpy.utils.logging import logger

from dcpy.utils import s3

SOCRATA_USER = os.getenv("SOCRATA_USER")
SOCRATA_PASSWORD = os.getenv("SOCRATA_PASSWORD")

SOCRATA_DOMAIN = "data.cityofnewyork.us"
SOCRATA_API_EP = f"https://{SOCRATA_DOMAIN}/api"
_revisions_root = f"{SOCRATA_API_EP}/publishing/v1/revision"
_views_root = f"{SOCRATA_API_EP}/views"

DISTRIBUTIONS_BUCKET = "edm-distributions"


def _simple_auth():
    return (SOCRATA_USER, SOCRATA_PASSWORD)


def _socrata_request(
    url,
    method: Literal["GET", "PUT", "POST", "PATCH", "DELETE"],
    **kwargs,
) -> dict:
    """Request wrapper to add auth, and raise exceptions on error."""
    request_fn = getattr(requests, method.lower())
    resp = request_fn(url, auth=_simple_auth(), **kwargs)
    if not resp.ok:
        raise Exception(resp.text)
    return resp.json()


class Socrata:
    """Helper classes for working with responses from/inputs to the Socrata Rest APIs.

    Some classes, e.g. the TypedDict subclasses, are here more as documentation.
    """

    class Inputs:
        """Helper classes to model inputs to the Socrata API."""

        class Attachment(TypedDict):
            name: str
            filename: str
            asset_id: str
            blob_id: str | None

    class Responses:
        """Helper classes to model responses from the Socrata API."""

        class Revision:
            def __init__(self, data):
                self._data = data
                self._raw_resource_data = data["resource"]
                self.revision_num = self._raw_resource_data["revision_seq"]
                self.four_four = self._raw_resource_data["fourfour"]
                self.metadata = self._raw_resource_data["metadata"]
                self.attachments = self._raw_resource_data["attachments"]

        class RevisionDataSource:
            def __init__(self, socrata_source):
                self.socrata_source = socrata_source

                # AR Note: It's not clear to me why/when you would have multiple schemas/output
                # schemas for any one datasource. In any case, I haven't yet encountered it,
                # and for our use case (single dataset upload per product) I don't expect we will
                self.soc_output_columns = socrata_source.attributes["schemas"][0][
                    "output_schemas"
                ][0]["output_columns"]

            @property
            def _column_update_endpoint(self):
                return (
                    f"https://{SOCRATA_DOMAIN}"
                    + self.socrata_source.input_schemas[0].links["transform"]
                )

            @property
            def column_names(self) -> set[str]:
                return {c["field_name"] for c in self.soc_output_columns}

            def update_column_metadata(self, dcp_cols):
                # TODO: changing column types. Not strictly required yet, and could be tricky
                logger.info(f"Updating Columns at {self._column_update_endpoint}")

                dcp_cols_by_name = {c["field_name"]: c for c in dcp_cols}
                dcp_col_names = [c["field_name"] for c in dcp_cols]

                assert (
                    dcp_cols_by_name.keys() == self.column_names
                ), "The field names in the uploaded source must match \
                    those in our metadata"

                for col in self.soc_output_columns:
                    # Input columns need to be matched to what's been uploaded,
                    # via the `initial_output_column_id`. Otherwise update
                    # requests are ignored.
                    col["initial_output_column_id"] = col["id"]
                    col["position"] = dcp_col_names.index(col["field_name"]) + 1

                    dcp_col = dcp_cols_by_name[col["field_name"]]

                    col["is_primary_key"] = dcp_col.get("is_primary_key") or False
                    col["display_name"] = dcp_col["display_name"]
                    col["description"] = dcp_col["description"]

                return _socrata_request(
                    self._column_update_endpoint,
                    "POST",
                    json={"output_columns": self.soc_output_columns},
                )


@dataclass(frozen=True)
class Dataset:
    four_four: str

    @property
    def revisions_endpoint(self):
        return f"{_revisions_root}/{self.four_four}"

    def fetch_metadata(self):
        """Fetch metadata (e.g. dataset name, tags) for a dataset."""
        return _socrata_request(f"{_views_root}/{self.four_four}.json", "GET")

    def fetch_open_revisions(self) -> list[Revision]:
        """Fetch all open revisions for a given dataset."""
        revs = _socrata_request(
            self.revisions_endpoint,
            "GET",
            params={"open": "true"},
        )
        return list(
            [Revision(r["resource"]["revision_seq"], self.four_four) for r in revs]
        )

    def discard_open_revisions(self):
        open_revs = self.fetch_open_revisions()
        logger.info(f"Discarding revisions: {[r.revision_num for r in open_revs]}")
        for r in open_revs:
            r.discard()

    def create_replace_revision(self) -> Revision:
        """Create a revision that will replace/overwrite the existing dataset, rather than upserting."""
        logger.info(f"Creating a revision at: {self.revisions_endpoint}")
        resp = _socrata_request(
            self.revisions_endpoint, "POST", json={"action": {"type": "replace"}}
        )["resource"]
        revision = Revision(revision_num=resp["revision_seq"], four_four=self.four_four)

        logger.info(
            f"Revision {revision.revision_num} created for {revision.four_four}"
        )
        return revision


@dataclass(frozen=True)
class Revision:
    revision_num: str
    four_four: str

    @property
    def revision_endpoint(self):
        return f"{_revisions_root}/{self.four_four}/{self.revision_num}"

    def apply(self):
        """Apply the revision to the dataset, closing the revision."""
        return _socrata_request(f"{self.revision_endpoint}/apply", "put")

    def discard(self):
        """Discard this revision."""
        return _socrata_request(self.revision_endpoint, "delete")

    def fetch_default_metadata(self) -> Socrata.Responses.Revision:
        """Fetch default metadata for a revision.

        AR Note: This doesn't return revision metadata that you've patched.
        e.g. If you patch an update to `contact email` for this revision, that change will not
        be reflected in the revision you fetch here, nor on the Socrata revision page.
        """
        return Socrata.Responses.Revision(
            _socrata_request(self.revision_endpoint, "GET")
        )

    def patch_metadata(
        self, metadata: dict, attachments: list[Socrata.Inputs.Attachment]
    ):
        return Socrata.Responses.Revision(
            _socrata_request(
                self.revision_endpoint,
                "PATCH",
                # TODO: should this header be a default?
                headers={
                    "Content-Type": "application/json",
                },
                data=json.dumps(
                    {
                        "attachments": attachments,
                        "metadata": metadata,
                    }
                ),
            )
        )

    def push_shp(
        self, path: Path, *, dest_filename: str | None = None
    ) -> Socrata.Responses.RevisionDataSource:
        # This is the only use of socrata-py. The data syncing utilities are nice.
        _socratapy_client = SocrataPy(
            Authorization(SOCRATA_DOMAIN, SOCRATA_USER, SOCRATA_PASSWORD)
        )
        view = _socratapy_client.views.lookup(self.four_four)
        rev = view.revisions.lookup(self.revision_num)
        with open(path, "rb") as shp_zip:
            logger.info(
                f"Pushing shapefiles at {path} to {self.four_four} - rev: {self.revision_num}"
            )
            push_resp = (
                rev.create_upload(dest_filename or shp_zip.name)
                .shapefile(shp_zip)
                .wait_for_finish()
            )
        error_details: dict | None = push_resp.attributes["failure_details"]
        if error_details:
            logger.error(f"Shapefile upload failed with {error_details}")
            raise Exception()
        return Socrata.Responses.RevisionDataSource(push_resp)

    def upload_attachment(
        self, path: Path, *, file_name: str | None = None
    ) -> Socrata.Inputs.Attachment:
        """Upload an attachment to a revision.

        Note: uploading an attachment isn't enough to truly attach it to a revision.
        It must also be patched into the metadata revision. Unfortunately, subsequent
        patches seem to wipe out the attachment metadata, everything must be gathered
        up into one patch.
        """
        # TODO: check other attachment types, e.g. xlsx
        content_types = {".csv": "text/csv", ".pdf": "application/pdf"}

        with open(path, "rb") as f:
            attachment_md_raw = _socrata_request(
                f"{self.revision_endpoint}/attachment",
                "POST",
                headers={
                    "X-File-Name": file_name or path.name,
                    "Content-type": content_types[path.suffix],
                },
                data=f,
            )
        return Socrata.Inputs.Attachment(
            # TODO: diff between name and filename? and what about blob_id?
            name=attachment_md_raw["filename"],
            filename=attachment_md_raw["filename"],
            asset_id=attachment_md_raw["file_id"],
            blob_id=None,
        )


def push_shp_from_s3(dataset_name: str, version: str):
    """Push Shapefile from edm-packaging to Socrata, and sync metadata."""
    dataset_metadata: dict = datasets[dataset_name]
    four_four: str = dataset_metadata["four_four"]
    socrata_metadata = dataset_metadata["metadata"]
    logger.info(
        f"Syncing shapefile and metadata for {dataset_name}/{version} to Socrata {four_four}"
    )

    download_root_path = Path(".library")
    dataset_path = download_root_path / dataset_name / version
    logger.info(
        f"Downloading shapefile from s3 dataset: {dataset_name} to {dataset_path.absolute()}"
    )

    s3.download_folder(
        DISTRIBUTIONS_BUCKET, f"{dataset_name}/{version}/", download_root_path
    )

    dataset = Dataset(four_four=four_four)
    dataset.discard_open_revisions()

    rev = dataset.create_replace_revision()

    data_source = rev.push_shp(path=dataset_path / "shapefile.zip")
    data_source.update_column_metadata(dcp_cols=dataset_metadata["columns"])

    attachments_metadata = [
        rev.upload_attachment(dataset_path / "attachments" / attachment)
        for attachment in dataset_metadata["attachments"]
    ]

    rev.patch_metadata(attachments=attachments_metadata, metadata=socrata_metadata)
    rev.apply()
    logger.info("Finished syncing product to Socrata")


app = typer.Typer(add_completion=False)


@app.command("push_shp_from_s3")
def _push_shp_from_s3_cli(
    dataset: str = typer.Option(
        None,
        "-d",
        "--dataset",
        help="Dataset name",
    ),
    version: str = typer.Option(
        None,
        "-v",
        "--version",
        help="Dataset version",
    ),
):
    return push_shp_from_s3(dataset, version)


@app.command("placeholder")
def _placeholder():
    # Adding a second command. If you only have one command defined,
    # typer ignores the specified command name.
    assert False, "Please don't invoke me."


if __name__ == "__main__":
    app()
