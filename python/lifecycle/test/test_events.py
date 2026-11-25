import pytest

from dcpy.lifecycle import events

EVENT = events.DatasetsArchived(
    store="edm.recipes.datasets",
    datasets=(events.ArchivedVersion("a", "v1"),),
    latest=False,
)


@pytest.fixture
def handlers(monkeypatch):
    """Injected handlers, in place of the entry point group."""
    injected: list = []
    monkeypatch.setattr(events, "_handlers", injected)
    return injected


def test_emit_without_handlers_does_nothing(handlers):
    events.emit(EVENT)


def test_emit_calls_handlers(handlers):
    seen = []
    handlers.append(("fake", seen.append))
    events.emit(EVENT)
    assert seen == [EVENT]


def test_failing_handler_only_warns(handlers):
    seen = []

    def broken(event):
        raise OSError("down")

    handlers.extend([("broken", broken), ("fake", seen.append)])
    events.emit(EVENT)
    assert seen == [EVENT]


def test_handlers_load_once(monkeypatch):
    calls = []
    monkeypatch.setattr(events, "_handlers", None)
    monkeypatch.setattr(events, "_discover", lambda: calls.append(1) or [])
    events.emit(EVENT)
    events.emit(EVENT)
    assert calls == [1]
