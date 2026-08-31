import pandas as pd
import pytest
from pages.gru import helpers
from pages.gru.constants import SourceDataset, source_datasets


def _source(upstream_kind: str | None = "template") -> SourceDataset:
    return SourceDataset(id="dcp_pad", refresh="", upstream_kind=upstream_kind)


def test_matching_versions_are_up_to_date():
    assert helpers.classify(_source(), "26c", "26c") == helpers.UP_TO_DATE


def test_differing_versions_are_behind():
    assert helpers.classify(_source(), "26b", "26c") == helpers.BEHIND


@pytest.mark.parametrize(
    "archived,upstream",
    [
        (ValueError("no config"), "26c"),
        ("26b", ValueError("origin unreachable")),
        # file_download templates have no connector that reports a version, so a source added
        # without an explicit upstream_key resolves to None rather than raising.
        ("26b", None),
    ],
)
def test_an_unreadable_version_is_unknown_not_behind(archived, upstream):
    """Sending someone to re-ingest over a failed fetch is worse than saying nothing."""
    assert helpers.classify(_source(), archived, upstream) == helpers.UNKNOWN


def test_a_source_with_no_archive_step_is_read_in_place():
    """dcp_saf is read straight out of edm-publishing, so it can never be behind."""
    assert helpers.classify(_source(upstream_kind=None), "26C", None) == (
        helpers.READ_IN_PLACE
    )


def test_only_behind_rows_are_offered_an_ingest():
    status = pd.DataFrame(
        [
            {"dataset": "a", "status": helpers.BEHIND},
            {"dataset": "b", "status": helpers.UP_TO_DATE},
            {"dataset": "c", "status": helpers.UNKNOWN},
            {"dataset": "d", "status": helpers.READ_IN_PLACE},
        ]
    )
    assert list(helpers.behind_sources(status)["dataset"]) == ["a"]


def test_every_dataset_the_checks_read_has_a_source_entry():
    """A check naming a source the registry doesn't know would silently drop it from the table."""
    from pages.gru.constants import qa_checks

    named = {source for sources in qa_checks["sources"] for source in sources}
    assert named <= set(source_datasets)


def test_bytes_sources_carry_an_explicit_key():
    """Their templates fetch a raw versioned URL, so the bytes connector needs the key spelled out."""
    for source in source_datasets.values():
        if source.upstream_kind == "bytes":
            assert source.upstream_key, source.id
