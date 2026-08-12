import pandas as pd
import pytest
from pages.distribution import helpers


def _versions(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows).set_index(["product", "dataset"])


def _row(
    product,
    dataset,
    destination_id="socrata",
    up_to_date=False,
    matches=True,
    bytes_known=True,
    open_data_known=True,
    readable=True,
):
    return {
        "product": product,
        "dataset": dataset,
        "destination_id": destination_id,
        "bytes_version": "26v1",
        "metadata_version": "26v1" if matches else "25v4",
        "open_data_versions": "25v4" if open_data_known else "",
        "up_to_date": up_to_date,
        "metadata_matches_bytes": matches,
        "bytes_version_known": bytes_known,
        "open_data_version_known": open_data_known,
        "open_data_version_readable": readable,
    }


def _outdated(versions):
    """The old single-bucket view, for tests about grouping rather than classification."""
    return helpers.needs_attention(versions).outdated


def test_up_to_date_rows_are_excluded():
    groups = _outdated(
        _versions(
            [
                _row("pluto", "pluto", up_to_date=True),
                _row("pluto", "mappluto", up_to_date=False),
            ]
        )
    )
    assert len(groups) == 1
    assert list(groups[0].ready["dataset"]) == ["mappluto"]


def test_datasets_of_a_product_batch_into_one_dispatch():
    groups = _outdated(_versions([_row("pluto", "pluto"), _row("pluto", "mappluto")]))
    assert len(groups) == 1
    assert sorted(groups[0].ready["dataset"]) == ["mappluto", "pluto"]


def test_destination_ids_do_not_batch_together():
    """The distribute CLI intersects --datasets with --destination-ids, so a batch spanning
    destination ids could redistribute an up-to-date pairing of the two."""
    groups = _outdated(
        _versions(
            [
                _row("dcm", "dcm", destination_id="socrata"),
                _row("dcm", "dcm_multi", destination_id="dcm_multilayer"),
            ]
        )
    )
    assert [(g.product, g.destination_id) for g in groups] == [
        ("dcm", "dcm_multilayer"),
        ("dcm", "socrata"),
    ]


def test_stale_metadata_version_blocks_a_dataset():
    groups = _outdated(
        _versions(
            [
                _row("pluto", "pluto", matches=True),
                _row("pluto", "mappluto", matches=False),
            ]
        )
    )
    assert list(groups[0].ready["dataset"]) == ["pluto"]
    assert list(groups[0].blocked["dataset"]) == ["mappluto"]


def test_group_with_nothing_ready_yields_no_datasets_to_dispatch():
    groups = _outdated(_versions([_row("pluto", "pluto", matches=False)]))
    assert groups[0].ready.empty
    assert list(groups[0].blocked["dataset"]) == ["pluto"]


def test_no_outdated_rows_yields_no_groups():
    assert _outdated(_versions([_row("pluto", "pluto", up_to_date=True)])) == []


def test_comparison_dataframe_composes_from_the_three_stages(monkeypatch):
    """The page fetches each source separately to report progress; make_comparison_dataframe has
    to assemble them without reaching back into product-metadata itself."""
    from dcpy.lifecycle.scripts import version_compare as vc

    # ConnectorRegistry is not a Mapping, so patch the dict it wraps.
    monkeypatch.setitem(
        vc.connectors._connectors,
        "bytes",
        type("FakeBytes", (), {"get_page_url": lambda self, key: f"bytes://{key}"})(),
    )
    snapshot = vc.MetadataSnapshot(
        keys=["pluto.pluto.socrata", "pluto.mappluto.socrata"],
        current_versions={
            "pluto.pluto.socrata": "25v4",  # matches Bytes -> distributable
            "pluto.mappluto.socrata": "25v3",  # stale -> blocked
        },
        four_fours={"pluto.pluto.socrata": "abcd-1234", "pluto.mappluto.socrata": None},
        version_tag_in_description={
            "pluto.pluto.socrata": True,
            "pluto.mappluto.socrata": True,
        },
    )

    df = vc.make_comparison_dataframe(
        snapshot,
        {"pluto.pluto": "25v4", "pluto.mappluto": "25v4"},
        {"pluto.pluto.socrata": "25v3", "pluto.mappluto.socrata": "25v3"},
    )
    row = df.loc[("pluto", "pluto")]

    assert row["metadata_version"] == "25v4"
    assert row["metadata_matches_bytes"]  # metadata caught up with Bytes
    assert not row["up_to_date"]  # but Open Data has not been pushed yet
    assert row["open_data_url"] == "https://data.cityofnewyork.us/d/abcd-1234"
    assert row["bytes_url"] == "bytes://pluto.pluto"

    groups = _outdated(vc.sort_by_outdated_products(df))
    assert len(groups) == 1
    assert list(groups[0].ready["dataset"]) == ["pluto"]
    assert list(groups[0].blocked["dataset"]) == ["mappluto"]


def _fake_bytes_connector(monkeypatch, vc):
    # ConnectorRegistry is not a Mapping, so patch the dict it wraps.
    monkeypatch.setitem(
        vc.connectors._connectors,
        "bytes",
        type("FakeBytes", (), {"get_page_url": lambda self, key: f"bytes://{key}"})(),
    )


def test_failed_fetches_do_not_reach_the_dataframe_as_exceptions(monkeypatch):
    """The fetchers record failures as the exception object. Left in the frame they make an
    `object` column Arrow can't serialize, which breaks rendering for every other row too."""
    pa = pytest.importorskip("pyarrow")
    from dcpy.lifecycle.scripts import version_compare as vc

    _fake_bytes_connector(monkeypatch, vc)
    snapshot = vc.MetadataSnapshot(
        keys=["pluto.pluto.socrata"],
        current_versions={"pluto.pluto.socrata": "25v4"},
        four_fours={"pluto.pluto.socrata": "abcd-1234"},
        version_tag_in_description={"pluto.pluto.socrata": True},
    )

    df = vc.make_comparison_dataframe(
        snapshot,
        {"pluto.pluto": KeyError("no sitemap entry")},
        {"pluto.pluto.socrata": "25v3"},
    )

    assert df.loc[("pluto", "pluto")]["bytes_version"] == "error: KeyError"
    # The real failure mode: st.dataframe hands the frame to pyarrow.
    pa.Table.from_pandas(df.reset_index())


def test_two_failed_fetches_are_not_treated_as_matching(monkeypatch):
    """Both sides erroring renders the same string on each; that must not read as up to date."""
    from dcpy.lifecycle.scripts import version_compare as vc

    _fake_bytes_connector(monkeypatch, vc)
    snapshot = vc.MetadataSnapshot(
        keys=["pluto.pluto.socrata"],
        current_versions={"pluto.pluto.socrata": "error: KeyError"},
        four_fours={"pluto.pluto.socrata": None},
        version_tag_in_description={"pluto.pluto.socrata": True},
    )

    row = vc.make_comparison_dataframe(
        snapshot,
        {"pluto.pluto": KeyError("boom")},
        {"pluto.pluto.socrata": KeyError("boom")},
    ).loc[("pluto", "pluto")]

    assert row["bytes_version"] == row["open_data_versions"] == "error: KeyError"
    assert not row["up_to_date"]
    assert not row["metadata_matches_bytes"]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("25v4", "25v4"),
        (None, ""),
        ([], ""),
        (KeyError("x"), "error: KeyError"),
    ],
)
def test_render_version_flattens_to_strings(value, expected):
    from dcpy.lifecycle.scripts.version_compare import render_version

    assert render_version(value) == expected


@pytest.mark.parametrize(
    "bytes_version, metadata_version, expected",
    [
        ("25v4", "25v4", True),
        ("25v4", "25v3", False),
        ("20260401", "202604", True),
        ("", "25v4", False),
    ],
)
def test_metadata_matches_bytes_uses_fuzzy_comparison(
    bytes_version, metadata_version, expected
):
    from dcpy.lifecycle.scripts.version_compare import FuzzyVersion

    assert (
        FuzzyVersion(bytes_version).probably_equals(FuzzyVersion(metadata_version))
        is expected
    )


def test_unreadable_open_data_version_is_not_reported_as_outdated():
    """A blank Open Data version means the description had no version to grep, not that the
    dataset is behind — the whole point of splitting the sections."""
    attention = helpers.needs_attention(
        _versions(
            [
                _row("pluto", "pluto", open_data_known=True),
                _row("cpdb", "cpdb", open_data_known=False, readable=True),
                _row(
                    "lion", "borough_boundaries", open_data_known=False, readable=False
                ),
            ]
        )
    )

    assert [g.product for g in attention.outdated] == ["pluto"]
    assert [g.product for g in attention.unconfirmed] == ["cpdb"]
    assert list(attention.no_version_tag["dataset"]) == ["borough_boundaries"]


def test_unreachable_bytes_version_is_not_reported_as_outdated():
    """Without a Bytes version there is nothing to compare against, so it cannot be `outdated`."""
    attention = helpers.needs_attention(
        _versions([_row("pluto", "pluto", bytes_known=False, readable=True)])
    )

    assert attention.outdated == []
    assert [g.product for g in attention.unconfirmed] == ["pluto"]


def test_a_readable_page_version_wins_over_missing_metadata_tag():
    """If a version was actually read off the page, trust it even where metadata lost the tag."""
    attention = helpers.needs_attention(
        _versions([_row("zoning", "zoning", open_data_known=True, readable=False)])
    )

    assert [g.product for g in attention.outdated] == ["zoning"]
    assert attention.no_version_tag.empty


def test_no_version_tag_rows_are_not_dispatchable():
    """They get no buttons: distributing cannot make an absent marker readable."""
    attention = helpers.needs_attention(
        _versions(
            [_row("lion", "atomic_polygons", open_data_known=False, readable=False)]
        )
    )

    assert attention.outdated == []
    assert attention.unconfirmed == []
    assert len(attention.no_version_tag) == 1


def test_cache_key_changes_when_the_frame_shape_does(monkeypatch):
    """A stale persisted frame served against new code is a real failure we hit; the cache key is
    derived from the shapes so it invalidates itself instead of needing a manual bump."""
    before = helpers._schema()
    monkeypatch.setattr(
        helpers, "REQUIRED_COLUMNS", helpers.REQUIRED_COLUMNS + ("newly_added",)
    )
    assert helpers._schema() != before


def test_cache_key_changes_when_the_snapshot_shape_does(monkeypatch):
    from dcpy.lifecycle.scripts import version_compare as vc

    before = helpers._schema()
    monkeypatch.setattr(
        vc.MetadataSnapshot, "_fields", vc.MetadataSnapshot._fields + ("newly_added",)
    )
    assert helpers._schema() != before


def test_missing_columns_report_a_stale_cache_rather_than_a_bare_keyerror():
    stale = _versions([_row("pluto", "pluto")]).drop(
        columns=["open_data_version_known"]
    )
    with pytest.raises(KeyError, match="Get versions"):
        helpers.needs_attention(stale)


def test_up_to_date_datasets_can_still_be_at_risk_of_losing_their_version():
    """The dangerous case: the page carries a version that product-metadata no longer does, so
    distributing would patch the weaker description over it. These read as up to date, so they
    are invisible unless surfaced separately."""
    attention = helpers.needs_attention(
        _versions(
            [
                _row("lion", "borough_boundaries", up_to_date=True, readable=False),
                _row("pluto", "pluto", up_to_date=True, readable=True),
            ]
        )
    )

    assert list(attention.would_lose_version["dataset"]) == ["borough_boundaries"]
    # and it is genuinely absent from every other bucket
    assert attention.outdated == []
    assert attention.unconfirmed == []
    assert attention.no_version_tag.empty


def test_datasets_with_no_readable_version_anywhere_are_not_at_risk():
    """Nothing to lose if the page has no version either — those belong in no_version_tag."""
    attention = helpers.needs_attention(
        _versions(
            [_row("lion", "atomic_polygons", open_data_known=False, readable=False)]
        )
    )

    assert attention.would_lose_version.empty
    assert list(attention.no_version_tag["dataset"]) == ["atomic_polygons"]
