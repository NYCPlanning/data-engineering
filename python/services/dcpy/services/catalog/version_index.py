"""Build planning's inputs from the catalog, as a dcpy.lifecycle.builds.version_index.VersionIndex.

One query for every input. Any input the catalog can't answer for exactly (no row, a row from a
different bucket or root folder, a row whose config.json couldn't be read) is left to the bucket,
and so is everything when the catalog is unreachable.
"""

from collections.abc import Sequence
from datetime import datetime

from dcpy import configuration
from dcpy.connectors.edm import recipes
from dcpy.connectors.edm.models import DatasetType
from dcpy.lifecycle.builds.version_index import InputRequest, ResolvedInput
from dcpy.utils.logging import logger

from . import db, ingest_versions
from .db import VersionRow

LATEST = "latest"


def _expected_location(key: str, version: str) -> str:
    """Where dcpy.connectors.edm.recipes looks for this version, as a catalog location."""
    return f"s3://{configuration.RECIPES_BUCKET}/{recipes.s3_folder_path(recipes.DatasetKey(id=key, version=version))}/"


def _resolved(
    row: VersionRow, preferences: Sequence[DatasetType]
) -> ResolvedInput | None:
    """Everything planning reads from the bucket for this row, or None to read it there.

    None also when the archive has none of the preferred file types, so the bucket path
    raises its usual error.
    """
    if ingest_versions.CONFIG_ERROR in row.details:
        return None
    if ingest_versions.ARCHIVED_AT_ISO not in row.details:
        return None
    file_type = next((t for t in preferences if t.value in row.file_types), None)
    if file_type is None:
        return None
    archived_iso = row.details[ingest_versions.ARCHIVED_AT_ISO]
    return ResolvedInput(
        version=row.version,
        file_type=file_type,
        name=row.details.get(ingest_versions.NAME),
        # The date in the offset the archive was written with, as the bucket path gives it.
        archive_date=datetime.fromisoformat(archived_iso).date()
        if archived_iso
        else None,
        url=row.details.get(ingest_versions.URL),
    )


class CatalogVersionIndex:
    """Reads the catalog's `ingest` rows for the store planning resolves inputs from."""

    def __init__(self, conn_url: str | None = None):
        self._conn_url = conn_url

    @staticmethod
    def from_rows(
        rows: Sequence[VersionRow],
        inputs: Sequence[InputRequest],
        preferences: Sequence[DatasetType],
    ) -> list[ResolvedInput | None]:
        # Only rows for the folder the bucket path would read.
        by_version = {
            (r.key, r.version): r
            for r in rows
            if r.location == _expected_location(r.key, r.version)
        }
        latest = {r.key: r.version for r in by_version.values() if r.is_latest}
        results: list[ResolvedInput | None] = []
        for i in inputs:
            version = latest.get(i.key) if i.version == LATEST else i.version
            row = by_version.get((i.key, version)) if version else None
            results.append(_resolved(row, preferences) if row else None)
        return results

    def resolve(
        self,
        source: str,
        inputs: Sequence[InputRequest],
        file_type_preference: Sequence[DatasetType],
    ) -> list[ResolvedInput | None]:
        if not inputs or not (self._conn_url or db.is_configured()):
            return [None] * len(inputs)
        try:
            rows = db.query(
                ingest_versions.DOMAIN,
                store=source,
                keys=sorted({i.key for i in inputs}),
                conn_url=self._conn_url,
            )
        except Exception as e:
            logger.warning(
                f"Catalog unavailable, resolving inputs from the bucket: {e}"
            )
            return [None] * len(inputs)
        return self.from_rows(rows, inputs, file_type_preference)
