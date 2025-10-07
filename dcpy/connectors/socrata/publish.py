"""DCP Socrata Connector to bridge the gaps between various Socrata APIs/SDKs.

The publishing API documentation is here:
https://dev.socrata.com/docs/other/publishing.html#?route=overview

Additionally, when it's mysterious about which endpoint to hit, or what the
arguments should be, opening the web interface and watching the requests is
quite informative.

TODO: This module should be "pure" - ie it should contain nothing DCP Specific.
This is more of a starting point for other connectors
"""

from __future__ import annotations
from dataclasses import dataclass
import json
import os
from pathlib import Path
from pydantic import BaseModel
import requests
from socrata.authorization import Authorization
from socrata import Socrata as SocrataPy
from socrata.output_schema import OutputSchema
from socrata.sources import Source
from socrata.revisions import Revision as SocrataPyRevision
import textwrap
from typing import TypedDict, Literal, NotRequired, Any

from dcpy.utils.logging import logger

import dcpy.models.product.dataset.metadata as md
import dcpy.models.dataset as dataset


# There are required publishing frequency fields in two different sections of
# the required metadata, and they're different. Below are the shared fields
UPDATE_SECTION_FREQUENCIES = {
    "Annually",
    "As needed",
    "Daily",
    "Every 10 years",
    "Every 2 months",
    "Every 2 weeks",
    "Every 2 years",
    "Every 3 years",
    "Every 4 months",
    "Every 4 years",
    "Every 5 years",
    "Every 6 months",
    "Every weekday",
    "Historical data",
    "Monthly",
    "Other",
    "Quarterly",
    "To be determined",
    "Weekly",
}
# Mapping of frequencies between the Legislative and Update sections.
# Not comprehensive, just values we might encounter and need to translate.
LEGISLATIVE_FREQUENCY_TO_UPDATE_FREQUENCY = {
    "As-Needed": "As needed",
    "Bimonthly": "Every 2 weeks",
    "Tri-annually": "Every 4 months",
    "Bi-annually": "Every 6 months",
    "Biennially": "Every 2 years",
    "Historical": "Historical data",
}


def translate_legislative_freq_to_update_freq(leg_freq: str):
    return (
        leg_freq
        if leg_freq in UPDATE_SECTION_FREQUENCIES
        else LEGISLATIVE_FREQUENCY_TO_UPDATE_FREQUENCY.get(leg_freq, "Other")
    )


class Socrata:
    """Helper classes for working with responses from/inputs to the Socrata Rest APIs.

    Some classes, e.g. the TypedDict subclasses, are here more as documentation.
    """

    class Inputs:
        """Helper classes to model inputs to the Socrata API."""

        class DatasetMetadata(BaseModel):
            name: str
            description: str
            category: str
            attribution: str
            attributionLink: str
            tags: list[str]
            # licenseId: str
            metadata: dict[str, Any]
            privateMetadata: dict[str, Any]

            @classmethod
            def from_dataset_attributes(cls, attrs: md.DatasetAttributes):
                if not (attrs.category and attrs.agency and attrs.publishing_frequency):
                    raise Exception(
                        f"Required metadata fields are missing. Found category: {attrs.category}, agency: {attrs.agency} or publishing_frequency: {attrs.publishing_frequency}"
                    )

                return cls(
                    name=attrs.display_name,
                    description=attrs.description,
                    category=attrs.category,
                    attribution=attrs.attribution or "",
                    attributionLink=attrs.attribution_link or "",
                    tags=attrs.tags or [],
                    metadata={
                        "rowLabel": attrs.each_row_is_a,
                        "custom_fields": {
                            "Dataset Information": {"Agency": attrs.agency},
                            "Update": {
                                "Data Change Frequency": attrs.publishing_frequency,
                                "Date Made Public": attrs.date_made_public,
                                "Update Frequency Details": attrs.publishing_frequency_details,
                                "Update Frequency": translate_legislative_freq_to_update_freq(
                                    attrs.publishing_frequency
                                )
                                or attrs.publishing_frequency,
                                "Automation": "Yes",
                            },
                        },
                    },
                    privateMetadata={
                        # "contactEmail": "", # Leaving this here in case we want to add it, so we don't have to remember what the field is called
                        "custom_fields": {
                            "Legislative Compliance": {
                                "Removed Records?": "Yes",  # refers to row removal at time of push to Socrata. Always true since we overwrite the existing dataset.
                                "Has Data Dictionary?": "Yes",
                                "Geocoded?": "Yes" if attrs.geocoded else "No",
                                "External Frequency (LL 110/2015)": attrs.publishing_frequency,
                                "Exists Externally? (LL 110/2015)": "Yes",
                                "Contains Address?": (
                                    "Yes" if attrs.contains_address else "No"
                                ),
                                "Can Dataset Feasibly Be Automated?": "Yes",
                                "Dataset from the Open Data Plan?": (
                                    "Yes"
                                    if attrs.custom.get("dataset_from_open_data_plan")
                                    else "No"
                                ),
                            },
                        },
                    },
                )

        class Column(BaseModel, extra="forbid"):
            name: str | None = None
            api_name: str | None = None
            display_name: str | None = None
            description: str | None = None
            is_primary_key: bool = False

            def __init__(self, col: md.DatasetColumn):
                self.name = col.id
                self.api_name = col.custom.get("api_name", col.id)
                self.display_name = col.name
                self.description = col.description
                self.is_primary_key = (
                    bool(col.checks.is_primary_key)
                    if isinstance(col.checks, dataset.Checks)
                    else False
                )

        class Attachment(TypedDict):
            name: str
            filename: str
            asset_id: str
            blob_id: str | None

    class Responses:
        """Helper classes to model responses from the Socrata API."""

        class Metadata(TypedDict):
            """Metadata returned from the Socrata API. Fields are not comprehensive."""

            id: str  # the four-four
            name: str
            columns: list[Socrata.Responses.Column]
            resourceName: str
            description: str
            tags: list[str]
            metadata: dict

        class Column(TypedDict):
            """Column fields returned from the Socrata API."""

            name: str  # ex: Project Name
            fieldName: str  # ex: projectnam
            description: str
            renderTypeName: str

            # cached attributes of a dataset, e.g. top 10 most common values,
            cachedContents: NotRequired[dict]

        class Revision:
            def __init__(self, data):
                self._data = data
                self._raw_resource_data = data["resource"]
                self.revision_num = self._raw_resource_data["revision_seq"]
                self.four_four = self._raw_resource_data["fourfour"]
                self.metadata = self._raw_resource_data["metadata"]
                self.attachments = self._raw_resource_data["attachments"]
                self.closed_at = self._raw_resource_data["closed_at"]


@dataclass(frozen=True)
class RevisionDataSource:
    """Wrapper for Socrata's Revision Data Source.
    A Data Source is what's returned when a dataset is pushed. It contains
    data about how to modify metadata for the pushed file,
    e.g. Column Names
    """

    MISSING_COLUMN_ERROR = (
        "The field names in the uploaded data do not match our metadata"
    )

    output_schema: OutputSchema
    _raw_socrata_source: Source | None = None

    @classmethod
    def from_socrata_source(cls, socrata_source: Source):
        return RevisionDataSource(
            output_schema=socrata_source.get_latest_input_schema().get_latest_output_schema(),
            _raw_socrata_source=socrata_source,
        )

    @property
    def column_names(self) -> list[str]:
        return [
            c["field_name"] for c in self.output_schema.attributes["output_columns"]
        ]

    def calculate_pushed_col_metadata(self, our_columns: list[md.DatasetColumn]):
        # TODO: using c.id or c.name?
        our_cols_by_field_name = {
            (c.custom.get("api_name") or c.id): c for c in our_columns
        }
        our_api_names = our_cols_by_field_name.keys()

        logger.info(
            f"""Calulating columns to push:
            Columns from dataset page: {sorted(self.column_names)}
            Columns from our metadata: {sorted(our_api_names)}
        """
        )

        if set(self.column_names) != our_api_names:
            raise Exception(
                {
                    "type": self.MISSING_COLUMN_ERROR,
                    # TODO: this should reference our column.ids in addition to the api_names
                    "missing_from_theirs": sorted(
                        our_api_names - set(self.column_names)
                    ),
                    "missing_from_ours": sorted(set(self.column_names) - our_api_names),
                }
            )

        failures = {
            col["field_name"]: col["transform"]["failure_details"]
            for col in self.output_schema.attributes["output_columns"]
            if col["transform"]["failure_details"]
        }
        if failures:
            raise Exception(
                "Socrata 'transformations' failed. See revision -> 'Review & Configure Data' -> 'Column Mapping'\n"
                f"Failures by columns:\n{textwrap.indent(json.dumps(failures, indent=4), '    ')}"
            )

        for col in self.output_schema.attributes["output_columns"]:
            # Take the Socrata metadata for columns that have been uploaded,
            # modify them to match our metadata.

            field_name = col["field_name"]
            our_col = our_cols_by_field_name[field_name]
            our_col_index = list(our_api_names).index(field_name)

            self.output_schema.change_column_metadata(field_name, "position").to(
                our_col_index + 1
            )

            self.output_schema.change_column_metadata(field_name, "is_primary_key").to(
                bool(our_col.checks.is_primary_key)
                if isinstance(our_col.checks, dataset.Checks)
                else False
            )

            self.output_schema.change_column_metadata(field_name, "display_name").to(
                our_col.name
            )
            self.output_schema.change_column_metadata(field_name, "description").to(
                our_col.description
            )

    def push_socrata_column_metadata(self, our_cols: list[md.DatasetColumn]):
        self.calculate_pushed_col_metadata(our_cols)
        return self.output_schema.run()


class AuthedSocrataResource:
    socrata_domain: str
    _socrata_user: str
    _socrata_password: str

    socrata_user_env_var: str = "SOCRATA_USER"
    socrata_user_pw_env_var: str = "SOCRATA_PASSWORD"

    def __init__(self, socrata_domain):
        self.socrata_domain = socrata_domain
        self._socrata_user = os.environ[self.socrata_user_env_var]
        self._socrata_password = os.environ[self.socrata_user_pw_env_var]

    def _simple_auth(self) -> tuple[str, str] | None:
        return (self._socrata_user, self._socrata_password)

    def _socrata_request(
        self,
        url,
        method: Literal["GET", "PUT", "POST", "PATCH", "DELETE"],
        **kwargs,
    ) -> dict:
        """Request wrapper to add auth, and raise exceptions on error."""
        request_fn = getattr(requests, method.lower())
        resp: requests.Response = request_fn(url, auth=self._simple_auth(), **kwargs)
        resp.raise_for_status()
        return resp.json()

    @property
    def socrata_api_ep(self):
        return f"https://{self.socrata_domain}/api"

    @property
    def views_root(self):
        return f"{self.socrata_api_ep}/views"

    @property
    def revisions_root(self):
        return f"{self.socrata_api_ep}/publishing/v1/revision"

    def _socratapy_client(self):
        return SocrataPy(
            Authorization(
                self.socrata_domain, self._socrata_user, self._socrata_password
            )
        )


class Dataset(AuthedSocrataResource):
    """Main class for interacting with the Socrata API.
    Retrieves creds from configurable env vars"""

    four_four: str

    def __init__(self, socrata_domain, four_four):
        super().__init__(socrata_domain)
        self.four_four = four_four

    def get_description(self) -> str:
        """Retrieve the description from the Socrata dataset metadata."""
        metadata = self.fetch_metadata()
        return metadata.get("description", "")

    @property
    def revisions_endpoint(self):
        return f"{self.revisions_root}/{self.four_four}"

    def fetch_metadata(self):
        """Fetch metadata (e.g. dataset name, tags) for a dataset."""
        return self._socrata_request(f"{self.views_root}/{self.four_four}.json", "GET")

    def make_revision_obj(self, revision_num: str):
        return Revision(
            four_four=self.four_four,
            socrata_domain=self.socrata_domain,
            revision_num=revision_num,
        )

    def fetch_open_revisions(self) -> list[Revision]:
        """Fetch all open revisions for a given dataset."""
        revs = self._socrata_request(
            self.revisions_endpoint,
            "GET",
            params={"open": "true"},
        )
        return list(
            [
                self.make_revision_obj(revision_num=r["resource"]["revision_seq"])
                for r in revs
            ]
        )

    def discard_open_revisions(self):
        open_revs = self.fetch_open_revisions()
        logger.info(f"Discarding revisions: {open_revs}")
        for rev in open_revs:
            rev.discard()

    def create_replace_revision(self) -> Revision:
        """Create a revision that will replace/overwrite the existing dataset, rather than upserting."""
        logger.info(f"Creating a revision at: {self.revisions_endpoint}")
        resp = self._socrata_request(
            self.revisions_endpoint, "POST", json={"action": {"type": "replace"}}
        )["resource"]
        revision = self.make_revision_obj(
            revision_num=resp["revision_seq"],
        )

        logger.info(
            f"Revision {revision.revision_num} created for {revision.four_four}"
        )
        return revision


class Revision(AuthedSocrataResource):
    revision_num: str
    four_four: str

    SOCRATA_REVISION_APPLY_TIMEOUT_SECS = 10 * 60  # Ten Mins

    def __init__(self, socrata_domain, four_four, revision_num):
        super().__init__(socrata_domain)
        self.four_four = four_four
        self.revision_num = revision_num

    @property
    def revisions_root(self):
        return f"{self.socrata_api_ep}/publishing/v1/revision"

    @property
    def revision_endpoint(self):
        return f"{self.revisions_root}/{self.four_four}/{self.revision_num}"

    @property
    def page_url(self):
        return f"https://{self.socrata_domain}/d/{self.four_four}/revisions/{self.revision_num}"

    def apply(self):
        """Apply the revision to the dataset, closing the revision."""
        return self._socrata_request(f"{self.revision_endpoint}/apply", "put")

    def discard(self):
        """Discard this revision."""
        return self._socrata_request(self.revision_endpoint, "delete")

    def fetch_default_metadata(self) -> Socrata.Responses.Revision:
        """Fetch default metadata for a revision.

        AR Note: This doesn't return revision metadata that you've patched.
        e.g. If you patch an update to `contact email` for this revision, that change will not
        be reflected in the revision you fetch here, nor on the Socrata revision page.
        """
        return Socrata.Responses.Revision(
            self._socrata_request(self.revision_endpoint, "GET")
        )

    def patch_metadata(
        self,
        metadata: Socrata.Inputs.DatasetMetadata,
        attachments: list[Socrata.Inputs.Attachment],
    ):
        return Socrata.Responses.Revision(
            self._socrata_request(
                self.revision_endpoint,
                "PATCH",
                # TODO: should this header be a default?
                headers={
                    "Content-Type": "application/json",
                },
                data=json.dumps(
                    {
                        "attachments": attachments,
                        "metadata": metadata.model_dump(),
                    }
                ),
            )
        )

    def _fetch_socratapy_revision(self) -> SocrataPyRevision:
        """Fetches the SocrataPy object wrapper around the revision object.
        This is useful for uploading to open revisions."""
        view = self._socratapy_client().views.lookup(self.four_four)
        return view.revisions.lookup(self.revision_num)

    def push_blob(self, path: Path, *, dest_filename: str):
        rev = self._fetch_socratapy_revision()
        with open(path, "rb") as blob:
            logger.info(
                f"Pushing blob at {path} to {self.four_four} - rev: {self.revision_num}"
            )
            push_resp = (
                rev.create_upload(dest_filename, {"parse_source": False})
                .blob(blob)
                .wait_for_finish()
            )
        error_details: dict | None = push_resp.attributes["failure_details"]
        if error_details:
            logger.error(f"BLOB upload failed with {error_details}")
            raise Exception(str(error_details))

    def push_csv(
        self, path: Path, *, dest_filename: str | None = None
    ) -> RevisionDataSource:
        rev = self._fetch_socratapy_revision()
        with open(path, "rb") as csv:
            logger.info(
                f"Pushing csv at {path} to {self.four_four} - rev: {self.revision_num}"
            )
            push_resp = (
                rev.create_upload(dest_filename or path.name).csv(csv).wait_for_finish()
            )
        error_details: dict | None = push_resp.attributes["failure_details"]
        if error_details:
            logger.error(f"CSV upload failed with {error_details}")
            raise Exception()
        return RevisionDataSource.from_socrata_source(push_resp)

    def push_shp(
        self, path: Path, *, dest_filename: str | None = None
    ) -> RevisionDataSource:
        rev = self._fetch_socratapy_revision()
        with open(path, "rb") as shp_zip:
            logger.info(
                f"Pushing shapefiles at {path} to {self.four_four} - rev: {self.revision_num}"
            )
            push_resp = (
                rev.create_upload(dest_filename or path.name)
                .shapefile(shp_zip)
                .wait_for_finish()
            )
        error_details: dict | None = push_resp.attributes["failure_details"]
        if error_details:
            logger.error(f"Shapefile upload failed with {error_details}")
            raise Exception()
        return RevisionDataSource.from_socrata_source(push_resp)

    def push_xlsx(
        self, path: Path, *, dest_filename: str | None = None
    ) -> RevisionDataSource:
        rev = self._fetch_socratapy_revision()
        with open(path, "rb") as xlsx:
            logger.info(
                f"Pushing xlsx at {path} to {self.four_four} - rev: {self.revision_num}"
            )
            push_resp = (
                rev.create_upload(dest_filename or path.name)
                .xlsx(xlsx)
                .wait_for_finish()
            )
        error_details: dict | None = push_resp.attributes["failure_details"]
        if error_details:
            logger.error(f"XLSX upload failed with {error_details}")
            raise Exception()
        return RevisionDataSource.from_socrata_source(push_resp)

    def upload_attachment(
        self,
        path: Path,
        *,
        dest_file_name: str,
    ) -> Socrata.Inputs.Attachment:
        """Upload an attachment to a revision.

        Note: uploading an attachment isn't enough to truly attach it to a revision.
        It must also be patched into the metadata revision. Unfortunately, subsequent
        patches seem to wipe out the attachment metadata, everything must be gathered
        up into one patch.
        """
        # TODO: check other attachment types, e.g. xlsx
        content_types = {
            ".csv": "text/csv",
            ".pdf": "application/pdf",
            ".xlsx": "application/octet-stream",
            ".xls": "application/octet-stream",
        }

        with open(path, "rb") as f:
            attachment_md_raw = self._socrata_request(
                f"{self.revision_endpoint}/attachment",
                "POST",
                headers={
                    "X-File-Name": dest_file_name,
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


SOCRATA_DESTINATION_TYPE = "socrata"
ERROR_WRONG_DESTINATION_TYPE = "Received a non-socrata type destination"
ERROR_MISSING_FOUR_FOUR = "The Socrata destination has no four-four"
ERROR_WRONG_DATASET_FILE_COUNT = (
    "The Socrata destination specifies the wrong number of dataset files."
)


class DestinationUses:
    attachment = "attachment"
    dataset_file = "dataset_file"


class SocrataDestination:
    dataset_file_id: str
    attachment_ids: set[str]
    four_four: str
    is_unparsed_dataset: bool = False

    def __init__(self, metadata: md.Metadata, destination_id: str):
        self.attachment_ids = set()
        dest = metadata.get_destination(destination_id)
        # if dest.type != SOCRATA_DESTINATION_TYPE:
        #     raise Exception(f"{ERROR_WRONG_DESTINATION_TYPE}: {dest.type}")

        self.four_four = dest.custom.get("four_four", "")
        if not self.four_four:
            raise Exception(ERROR_MISSING_FOUR_FOUR)

        dataset_file_ids = []
        for f in metadata.get_destination(destination_id).files:
            match f.custom["destination_use"]:
                case DestinationUses.attachment:
                    self.attachment_ids.add(f.id)
                case DestinationUses.dataset_file:
                    dataset_file_ids.append(f.id)
        if len(dataset_file_ids) != 1:
            raise Exception(f"{ERROR_WRONG_DATASET_FILE_COUNT}: {dataset_file_ids}")
        self.dataset_file_id = dataset_file_ids[0]
        self.is_unparsed_dataset = dest.custom.get("is_unparsed_dataset", False)
