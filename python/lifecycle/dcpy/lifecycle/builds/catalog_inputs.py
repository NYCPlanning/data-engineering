"""Recipe input details from the version catalog, for planning.

Planning resolves, for each edm.recipes.datasets input, its latest version, file type, name,
archive date and source url: five or so bucket calls per input. With the version catalog
configured (DCPY_CATALOG_PG_URL) it gets them for every input in one query instead. Any input the
catalog can't answer for exactly (no row, a row from a different bucket or root folder, a row
whose config.json couldn't be read) is resolved from the bucket as before, and so is everything
when the catalog is unreachable: a stale catalog can slow planning down, never break it.
"""

from dataclasses import dataclass
from datetime import date, datetime

from dcpy import configuration
from dcpy.connectors.edm import catalog, recipes
from dcpy.connectors.edm.catalog import VersionRow, ingest_versions
from dcpy.connectors.edm.models import DatasetType
from dcpy.utils.logging import logger


@dataclass(frozen=True)
class InputDetails:
    file_type: DatasetType
    name: str | None
    archive_date: date | None
    url: str | None


def _expected_location(key: str, version: str) -> str:
    """Where dcpy.connectors.edm.recipes looks for this version, as a catalog location."""
    return f"s3://{configuration.RECIPES_BUCKET}/{recipes.s3_folder_path(recipes.DatasetKey(id=key, version=version))}/"


class CatalogInputs:
    def __init__(self, rows: list[VersionRow]):
        # Only rows for the folder the bucket path would read.
        self._rows = {
            (r.key, r.version): r
            for r in rows
            if r.location == _expected_location(r.key, r.version)
        }
        self._latest = {r.key: r.version for r in self._rows.values() if r.is_latest}

    @staticmethod
    def load(store: str, keys: list[str]) -> "CatalogInputs | None":
        """The catalog's rows for these keys, or None (use the bucket) when it's off or fails."""
        if not keys or not catalog.is_configured():
            return None
        try:
            rows = catalog.query(ingest_versions.DOMAIN, store=store, keys=keys)
        except Exception as e:
            logger.warning(
                f"Catalog unavailable, resolving inputs from the bucket: {e}"
            )
            return None
        return CatalogInputs(rows)

    def latest_version(self, key: str) -> str | None:
        return self._latest.get(key)

    def details(
        self, key: str, version: str, preferences: list[DatasetType]
    ) -> InputDetails | None:
        """Everything planning reads from the bucket for this version, or None to read it there.

        None also when the archive has none of the preferred file types, so the bucket path
        raises its usual error.
        """
        row = self._rows.get((key, version))
        if row is None or ingest_versions.CONFIG_ERROR in row.details:
            return None
        if ingest_versions.ARCHIVED_AT_ISO not in row.details:
            return None
        file_type = next((t for t in preferences if t.value in row.file_types), None)
        if file_type is None:
            return None
        archived_iso = row.details[ingest_versions.ARCHIVED_AT_ISO]
        return InputDetails(
            file_type=file_type,
            name=row.details.get(ingest_versions.NAME),
            # The date in the offset the archive was written with, as the bucket path gives it.
            archive_date=datetime.fromisoformat(archived_iso).date()
            if archived_iso
            else None,
            url=row.details.get(ingest_versions.URL),
        )
