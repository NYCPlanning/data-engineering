from dataclasses import dataclass
from pathlib import Path

from streamlit.runtime.uploaded_file_manager import UploadedFile

from dcpy.lifecycle.ingest import get_template_directory, list_ingest_templates
from dcpy.lifecycle.ingest.connectors import get_processed_datastore_connector
from dcpy.lifecycle.ingest.models import DataSourceDefinition
from dcpy.lifecycle.ingest.plan import read_definition_file
from dcpy.utils import s3

# A template opts into this upload flow just by pointing its source key under this prefix in
# edm-private - no separate registry of "inbox-enabled" datasets to keep in sync.
INBOX_MARKER = "qa_app/inbox/"


def list_inbox_datasets() -> list[str]:
    """Ingest template ids whose source lives under edm-private/qa_app/inbox/."""
    return sorted(
        template.name
        for template in list_ingest_templates()
        if INBOX_MARKER in template.path.read_text()
    )


@dataclass
class ResolvedInboxUpload:
    bucket: str
    key: str
    # Id to check on the *processed* datastore connector for "has this version already been
    # fully ingested" - a source-with-many-downstream-datasets template (e.g. a multi-layer
    # gdb) archives the raw file under a timestamp, not the version typed in here, so the
    # clean version string only ever shows up on its downstream datasets. Any one of them
    # works as a stand-in, since they're all archived together in the same ingest run.
    processed_check_id: str


def _read_definition(dataset_id: str, version: str):
    template_path = get_template_directory() / f"{dataset_id}.yml"
    return read_definition_file(template_path, version=version)


def _processed_check_id(definition) -> str:
    return (
        definition.datasets[0].id
        if isinstance(definition, DataSourceDefinition)
        else definition.id
    )


def get_inbox_bucket(dataset_id: str) -> str:
    """The bucket this dataset's template uploads to - a static field, so any placeholder
    version resolves it without needing a real one."""
    definition = _read_definition(dataset_id, version="_probe_")
    bucket = getattr(definition.source, "bucket", None)
    if not bucket:
        raise ValueError(f"Ingest template '{dataset_id}' has no source.bucket set")
    return bucket


def list_inbox_versions(dataset_id: str) -> list[str]:
    """Versions already uploaded to this dataset's inbox folder, newest first.

    Sorted case-insensitively: version casing isn't normalized on upload (datasets can have
    their own conventions), but a stray different-cased version shouldn't sort as if it were
    a wildly different value than its same-letters counterpart - e.g. "26a" and "26A" should
    land next to each other, not at opposite ends of the list.
    """
    bucket = get_inbox_bucket(dataset_id)
    prefix = f"{INBOX_MARKER}{dataset_id}/"
    return sorted(s3.get_subfolders(bucket, prefix), key=str.lower, reverse=True)


def resolve_destination(dataset_id: str, version: str) -> ResolvedInboxUpload:
    """Resolve where this dataset+version's ingest template expects its source uploaded."""
    definition = _read_definition(dataset_id, version)
    source = definition.source
    bucket = getattr(source, "bucket", None)
    if not bucket:
        raise ValueError(f"Ingest template '{dataset_id}' has no source.bucket set")
    return ResolvedInboxUpload(
        bucket=bucket,
        key=source.key,
        processed_check_id=_processed_check_id(definition),
    )


def expected_filename(key: str) -> str:
    return Path(key).name


def is_in_inbox(resolved: ResolvedInboxUpload) -> bool:
    return s3.object_exists(resolved.bucket, resolved.key)


def is_fully_ingested(dataset_id: str, version: str) -> bool:
    """True if this version has already been archived to the processed datastore."""
    definition = _read_definition(dataset_id, version)
    check_id = _processed_check_id(definition)
    return get_processed_datastore_connector().version_exists(check_id, version)


def upload(bucket: str, key: str, uploaded_file: UploadedFile) -> None:
    """Push the uploaded file straight to its resolved inbox location.

    UploadedFile is itself an in-memory file object (a BytesIO subclass) Streamlit already
    buffers, so this pushes it directly rather than copying it into a second buffer first -
    worth avoiding now that uploads run up to a couple GB. Nothing is written to disk here,
    so there's nothing to clean up afterward either.
    """
    s3.upload_file_obj(uploaded_file, bucket, key, "private")
