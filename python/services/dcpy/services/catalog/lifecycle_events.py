"""The catalog's dcpy.lifecycle.events handler (entry point `dcpy.lifecycle.event_handlers`).

On DatasetsArchived, re-reads what's archived for those datasets and updates their rows. Does
nothing when the catalog isn't configured. Lifecycle logs anything raised here as a warning, and
the next full refresh (ingest_versions.refresh_store) picks up whatever was missed.
"""

from dcpy.connectors.ingest_datastore import Connector as IngestDatastore
from dcpy.lifecycle import events
from dcpy.lifecycle.connector_registry import connectors
from dcpy.utils.logging import logger

from . import db, ingest_versions


def record_archived(event: events.DatasetsArchived) -> db.WriteCounts:
    keys = sorted({d.key for d in event.datasets})
    counts = ingest_versions.record_keys(
        connectors[event.store, IngestDatastore], event.store, keys
    )
    logger.info(
        f"Catalog: recorded {', '.join(keys)} in {event.store}: {counts.added} added, "
        f"{counts.updated} updated, {counts.unchanged} unchanged"
    )
    return counts


def handle(event: events.Event) -> None:
    if not db.is_configured():
        return
    if isinstance(event, events.DatasetsArchived) and event.datasets:
        record_archived(event)
