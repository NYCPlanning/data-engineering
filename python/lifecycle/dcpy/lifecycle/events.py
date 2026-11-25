"""Lifecycle events: what a stage did, for whoever's listening.

Stages call `emit(event)` unconditionally after doing something other systems may want to
know about (e.g. archiving datasets). Handlers are found through the `dcpy.lifecycle.event_handlers`
entry point group, so a package installed alongside lifecycle can listen without lifecycle
knowing it exists:

    [project.entry-points."dcpy.lifecycle.event_handlers"]
    my_handler = "my.package.module:handle"

A handler is a callable taking one event; it should ignore event types it doesn't handle. With no
handlers installed, `emit` does nothing. A handler that raises is logged as a warning and never
fails the stage that emitted.
"""

from collections.abc import Callable
from dataclasses import dataclass
from importlib.metadata import entry_points

from dcpy.utils.logging import logger

ENTRY_POINT_GROUP = "dcpy.lifecycle.event_handlers"


@dataclass(frozen=True)
class Event:
    pass


@dataclass(frozen=True)
class ArchivedVersion:
    key: str
    version: str


@dataclass(frozen=True)
class DatasetsArchived(Event):
    """Ingest pushed these versions to a datasets store (or pointed latest at them)."""

    # registry name of the connector the datasets were pushed to, e.g. edm.recipes.datasets
    store: str
    datasets: tuple[ArchivedVersion, ...]
    latest: bool


Handler = Callable[[Event], None]

# Loaded on first emit. Tests replace it to inject handlers without real entry points.
_handlers: list[tuple[str, Handler]] | None = None


def _discover() -> list[tuple[str, Handler]]:
    handlers = []
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        try:
            handlers.append((ep.name, ep.load()))
        except Exception as e:
            logger.warning(f"Couldn't load lifecycle event handler {ep.name}: {e}")
    return handlers


def handlers() -> list[tuple[str, Handler]]:
    global _handlers
    if _handlers is None:
        _handlers = _discover()
    return _handlers


def emit(event: Event) -> None:
    for name, handle in handlers():
        try:
            handle(event)
        except Exception as e:
            logger.warning(
                f"Lifecycle event handler {name} failed on {type(event).__name__}: {e}"
            )
